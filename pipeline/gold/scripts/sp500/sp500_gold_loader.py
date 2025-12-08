from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import yfinance as yf
from airflow.utils.log.logging_mixin import LoggingMixin
from config import Config
from rapidfuzz import fuzz, process
from utils import aws_utils


class SP500GoldLoader(LoggingMixin):
    """GOLD loader: enriches SP500 Silver data with daily OHLC prices."""

    def __init__(self):
        """Initialize SP500 Gold Loader with S3 paths."""
        self.s3 = aws_utils.S3Utils()
        self.sp500_silver_prefix = (
            f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SP500}"
        )
        self.spy_silver_prefix = (
            f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SPY}"
        )
        self.gold_prefix = f"{Config.S3_GOLD_DIR_PREFIX}/sp500_daily_prices"

    def load_sp500_silver(self, date_path):
        """Load SP500 Silver parquet for the given date."""
        prefix = f"{self.sp500_silver_prefix}/{date_path}/"
        keys = aws_utils.list_s3_keys(Config.S3_BUCKET, prefix)

        if not keys:
            raise FileNotFoundError(f"No SP500 silver found at: {prefix}")

        key = sorted(keys)[-1]
        self.log.info(f"Loading SP500 Silver: s3://{Config.S3_BUCKET}/{key}")
        return self.s3.read_parquet(Config.S3_BUCKET, key)

    def load_spy_silver(self, date_path):
        """Load SPY Silver parquet (Top 10 holdings)."""
        prefix = f"{self.spy_silver_prefix}/{date_path}/"
        keys = aws_utils.list_s3_keys(Config.S3_BUCKET, prefix)

        if not keys:
            raise FileNotFoundError(f"No SPY silver found at: {prefix}")

        key = sorted(keys)[-1]
        self.log.info(f"Loading SPY Silver: s3://{Config.S3_BUCKET}/{key}")
        return self.s3.read_parquet(Config.S3_BUCKET, key)

    def enrich_with_prices(self, df):
        """Fetch daily prices and enrich SPY Top 10 holdings."""
        prices = {}
        prev = {}

        for symbol in df["Symbol"].dropna().unique():
            try:
                t = yf.Ticker(symbol)
                hist = t.history(period="2d")
                if len(hist) >= 2:
                    prev_close = hist["Close"].iloc[-2]
                    last_price = hist["Close"].iloc[-1]
                else:
                    prev_close = last_price = None
            except Exception:
                prev_close = last_price = None

            prices[symbol] = last_price
            prev[symbol] = prev_close

        df["LastPrice"] = df["Symbol"].map(prices)
        df["PrevClose"] = df["Symbol"].map(prev)
        df["DailyChangePct"] = (
            (df["LastPrice"] - df["PrevClose"]) / df["PrevClose"]
        ) * 100

        return df

    def enrich(self, sp500_df, spy_df):
        """Enrich SP500 data with SPY Top 10 and daily prices."""
        self.log.info("Starting GOLD enrichment...")

        sp500_df.columns = [c.lower() for c in sp500_df.columns]
        self.log.info("SP500 columns normalized.")
        sp500_df.head()
        spy_df.columns = [c.lower().replace(" ", "_") for c in spy_df.columns]
        self.log.info("SPY columns normalized.")
        spy_df.head()

        if "symbol" not in sp500_df.columns:
            self.log.error("No 'symbol' column found in SP500 DataFrame")
            raise ValueError("No 'symbol' column found in SP500 DataFrame")

        spy_df["symbol"] = spy_df["name"].apply(
            lambda x: self.map_to_symbol(x, sp500_df)
        )

        enriched = sp500_df.merge(
            spy_df, on="symbol", how="left", validate="many_to-one"
        )

        enriched["holding_percent"] = enriched["holding_percent"].fillna(0)

        enriched = enriched.sort_values(["date", "symbol"]).reset_index(drop=True)

        # Enriquecer con precios
        enriched = self.enrich_with_prices(enriched)

        return enriched

    def map_to_symbol(self, spy_name, sp500_df):
        """Map SPY company name to SP500 symbol using fuzzy matching."""
        sp500_names = sp500_df[
            "security"
        ].tolist()  # 'security' contains the company names
        sp500_symbols = sp500_df[
            "symbol"
        ].tolist()  # 'symbol' contains the corresponding symbols

        match, score, idx = process.extractOne(
            spy_name, sp500_names, scorer=fuzz.WRatio
        )

        if score >= 85:
            return sp500_symbols[idx]
        else:
            return None  # If no good match, return None

    def save_gold(self, df, execution_date):
        """Save the enriched DataFrame to S3 in GOLD layer."""
        date_path = execution_date.strftime("%Y/%m/%d")
        output_key = f"{self.gold_prefix}/{date_path}/sp500_gold.parquet"

        self.log.info(f"Saving GOLD dataset → s3://{Config.S3_BUCKET}/{output_key}")

        fs = s3fs.S3FileSystem()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f"s3://{Config.S3_BUCKET}/{output_key}", filesystem=fs)

        self.log.info("GOLD dataset successfully written.")

    def run(self):
        """Run the SP500 Gold Loader process."""
        execution_date = datetime.utcnow()
        date_path = execution_date.strftime("%Y/%m/%d")
        self.log.info(f"Executing GOLD load for {date_path}...")

        sp500_df = self.load_sp500_silver(date_path)
        spy_df = self.load_spy_silver(date_path)

        gold_df = self.enrich(sp500_df, spy_df)
        self.save_gold(gold_df, execution_date)

        self.log.info("===== GOLD LOAD COMPLETED SUCCESSFULLY =====")
