from datetime import datetime
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import yfinance as yf
from airflow.utils.log.logging_mixin import LoggingMixin
from config import Config
from rapidfuzz import fuzz, process
from utils import aws_utils


class SP500GoldLoader(LoggingMixin):
    """GOLD loader: Enrich Silver SP500 and SPY Top 10 with symbols, prices and daily variation."""

    def __init__(self):
        super().__init__()
        self.s3 = aws_utils.S3Utils()

        self.sp500_silver_prefix = (
            f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SP500}"
        )
        self.spy_silver_prefix = (
            f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SPY}"
        )
        self.gold_prefix = (
            f"{Config.S3_GOLD_DIR_PREFIX}/{Config.S3_GOLD_PREFIX_SP500_DAILY_PRICES}"
        )

    def load_sp500_silver(self, date_path: str) -> pd.DataFrame:
        """Load the latest SP500 silver file for the given date path."""
        prefix = f"{self.sp500_silver_prefix}/{date_path}/"
        keys = aws_utils.list_s3_keys(Config.S3_BUCKET, prefix)

        if not keys:
            raise FileNotFoundError(f"No SP500 silver files found at: {prefix}")

        key = sorted(keys)[-1]
        self.log.info(f"Loading SP500 Silver file: s3://{Config.S3_BUCKET}/{key}")
        return self.s3.read_parquet(Config.S3_BUCKET, key)

    def load_spy_silver(self, date_path: str) -> pd.DataFrame:
        """Load the latest SPY silver file for the given date path."""
        prefix = f"{self.spy_silver_prefix}/{date_path}/"
        keys = aws_utils.list_s3_keys(Config.S3_BUCKET, prefix)

        if not keys:
            raise FileNotFoundError(f"No SPY silver files found at: {prefix}")

        key = sorted(keys)[-1]
        self.log.info(f"Loading SPY Silver file: s3://{Config.S3_BUCKET}/{key}")
        return self.s3.read_parquet(Config.S3_BUCKET, key)

    def map_to_symbol(self, spy_name: str, sp500_df: pd.DataFrame) -> Optional[str]:
        """Map SPY holding name to SP500 symbol using fuzzy matching."""
        if pd.isna(spy_name) or not spy_name:
            return None

        sp500_names = sp500_df["security"].dropna().tolist()
        sp500_symbols = sp500_df["symbol"].dropna().tolist()

        match_result = process.extractOne(spy_name, sp500_names, scorer=fuzz.WRatio)

        if match_result:
            matched_name, score, idx = match_result
            if score >= 85:
                self.log.debug(
                    f"Matched '{spy_name}' to '{matched_name}' with symbol '{sp500_symbols[idx]}' (score: {score})"
                )
                return sp500_symbols[idx]
            else:
                self.log.debug(f"No good match for '{spy_name}' (best score: {score})")
                return None
        else:
            return None

    def enrich_with_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add today price, yesterday price, and daily % variation per symbol."""
        prices = {}
        prev_closes = {}

        unique_symbols = df["symbol"].dropna().unique()
        self.log.info(f"Fetching prices for {len(unique_symbols)} symbols...")

        for symbol in unique_symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="2d")
                if len(hist) >= 2:
                    prev_closes[symbol] = hist["Close"].iloc[-2]
                    prices[symbol] = hist["Close"].iloc[-1]
                else:
                    self.log.warning(f"Not enough history for symbol {symbol}")
                    prices[symbol] = None
                    prev_closes[symbol] = None
            except Exception as e:
                self.log.warning(f"Error fetching price for {symbol}: {e}")
                prices[symbol] = None
                prev_closes[symbol] = None

        df["price_today"] = df["symbol"].map(prices)
        df["price_yesterday"] = df["symbol"].map(prev_closes)
        df["variation_percent"] = (
            (df["price_today"] - df["price_yesterday"]) / df["price_yesterday"] * 100
        ).round(4)

        return df

    def enrich(self, sp500_df: pd.DataFrame, spy_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich SPY Top 10 holdings with symbols, prices and daily variation."""
        self.log.info("Starting SPY Top 10 GOLD enrichment...")

        sp500_df.columns = [c.lower().replace(" ", "_") for c in sp500_df.columns]
        spy_df.columns = [c.lower().replace(" ", "_") for c in spy_df.columns]

        # Map SPY holdings -> symbol
        spy_df["symbol"] = spy_df["name"].apply(
            lambda n: self.map_to_symbol(n, sp500_df)
        )

        # Keep only successfully matched holdings (usually 10)
        spy_df = spy_df[spy_df["symbol"].notnull()]
        self.log.info(f"Matched {len(spy_df)} SPY holdings")

        # Add prices only for the 10 holdings
        spy_df = self.enrich_with_prices(spy_df)

        # GOLD returns only these 10 rows
        return spy_df[
            ["symbol", "name", "holding_percent", "price_today", "variation_percent"]
        ].reset_index(drop=True)

    def save_gold(self, df: pd.DataFrame, execution_date: datetime):
        """Save GOLD dataframe to S3 in parquet format."""
        date_path = execution_date.strftime("%Y/%m/%d")
        output_key = (
            f"{self.gold_prefix}/{date_path}/{Config.S3_GOLD_DAILY_PRICES}.parquet"
        )

        self.log.info(f"Saving GOLD dataset to s3://{Config.S3_BUCKET}/{output_key}")

        fs = s3fs.S3FileSystem()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f"s3://{Config.S3_BUCKET}/{output_key}", filesystem=fs)

        self.log.info("GOLD dataset saved successfully.")

    def run(self):
        """Run the GOLD enrichment process."""
        execution_date = datetime.utcnow()
        date_path = execution_date.strftime("%Y/%m/%d")

        self.log.info("===== Starting GOLD load =====")
        self.log.info(f"Processing date path: {date_path}")

        sp500_df = self.load_sp500_silver(date_path)
        spy_df = self.load_spy_silver(date_path)

        gold_df = self.enrich(sp500_df, spy_df)
        self.log.info(f"Enriched GOLD dataset: {gold_df.shape[0]} rows")
        self.log.info(f"Enriched GOLD dataset columns: {gold_df.columns.tolist()}")
        self.log.info(f"Enriched GOLD dataset sample:\n{gold_df.head()}")
        self.save_gold(gold_df, execution_date)

        self.log.info("===== GOLD load completed successfully =====")


def main():
    """Run the SP500 GOLD loader."""
    SP500GoldLoader().run()


if __name__ == "__main__":
    main()
