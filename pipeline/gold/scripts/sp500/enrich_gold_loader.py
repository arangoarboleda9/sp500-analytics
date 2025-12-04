"""SP500 GOLD Loader Script - Solo SP500 y SPY Silver."""

from datetime import datetime
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from airflow.utils.log.logging_mixin import LoggingMixin
from config import Config
from rapidfuzz import fuzz, process
from utils import aws_utils


class SP500GoldLoader(LoggingMixin):
    """
    Genera el GOLD dataset combinando:

    - Silver SP500 daily % changes
    - Silver SPY Top 10 holdings
    """

    def __init__(self):
        """Iniatialize the SP500 GOLD Loader."""
        super().__init__()
        self.gold_prefix = f"{Config.S3_GOLD_DIR_PREFIX}/sp500_daily_prices"
        self.s3 = aws_utils.S3Utils()

        self.sp500_silver_prefix = (
            f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SP500}"
        )
        self.spy_silver_prefix = (
            f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SPY}"
        )

    def load_sp500_silver(self, date_path: str) -> pd.DataFrame:
        """Load the SP500 silver dataset from S3 for the given date path."""
        prefix = f"{self.sp500_silver_prefix}/{date_path}/"
        keys = aws_utils.list_s3_keys(Config.S3_BUCKET, prefix)

        if not keys:
            raise FileNotFoundError(f"No SP500 silver found at: {prefix}")

        key = sorted(keys)[-1]
        self.log.info(f"Loading SP500 Silver: s3://{Config.S3_BUCKET}/{key}")
        return self.s3.read_parquet(Config.S3_BUCKET, key)

    def load_spy_silver(self, date_path: str) -> pd.DataFrame:
        """Load the SPY silver dataset from S3 for the given date path."""
        prefix = f"{self.spy_silver_prefix}/{date_path}/"
        keys = aws_utils.list_s3_keys(Config.S3_BUCKET, prefix)

        if not keys:
            raise FileNotFoundError(f"No SPY silver found at: {prefix}")

        key = sorted(keys)[-1]
        self.log.info(f"Loading SPY Silver: s3://{Config.S3_BUCKET}/{key}")
        return self.s3.read_parquet(Config.S3_BUCKET, key)

    def enrich(self, sp500_df: pd.DataFrame, spy_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich SP500 data with SPY holdings information."""
        self.log.info("Starting GOLD enrichment...")

        sp500_df.columns = [c.lower() for c in sp500_df.columns]
        spy_df.columns = [c.lower().replace(" ", "_") for c in spy_df.columns]

        self.log.info(f"SP500 columns after normalization: {list(sp500_df.columns)}")
        self.log.info(f"SPY columns after normalization: {list(spy_df.columns)}")

        if "symbol" not in sp500_df.columns:
            self.log.error(
                f"No 'symbol' column found in SP500 DataFrame. Available columns: {list(sp500_df.columns)}"
            )
            raise ValueError("No 'symbol' column found in SP500 DataFrame")

        if "name" not in spy_df.columns:
            self.log.error(
                f"No 'name' column found in SPY DataFrame. Available columns: {list(spy_df.columns)}"
            )
            raise ValueError("No 'name' column found in SPY DataFrame")

        self.log.info("Mapping SPY names to SP500 symbols...")
        spy_df["symbol"] = spy_df["name"].apply(
            lambda x: self.map_to_symbol(x, sp500_df)
        )

        valid_symbols = spy_df["symbol"].notna().sum()
        self.log.info(
            f"Mapped {valid_symbols} out of {len(spy_df)} SPY holdings to symbols"
        )

        enriched = sp500_df.merge(
            spy_df,
            on="symbol",
            how="left",
            validate="many_to_one",
        )

        if "holding_percent" in enriched.columns:
            enriched["holding_percent"] = enriched["holding_percent"].fillna(0)
        else:
            enriched["holding_percent"] = 0

        sort_columns = []
        if "date" in enriched.columns:
            sort_columns.append("date")
        if "symbol" in enriched.columns:
            sort_columns.append("symbol")

        if sort_columns:
            enriched = enriched.sort_values(sort_columns).reset_index(drop=True)
        else:
            enriched = enriched.reset_index(drop=True)

        return enriched

    def map_to_symbol(self, spy_name: str, sp500_df: pd.DataFrame) -> Optional[str]:
        """Map SPY company name to SP500 symbol using fuzzy matching."""
        if pd.isna(spy_name) or not spy_name:
            return None

        if "security" not in sp500_df.columns:
            self.log.warning(
                "No 'security' column found in SP500 DataFrame for name matching"
            )
            return None

        sp500_names = sp500_df["security"].dropna().tolist()
        sp500_symbols = sp500_df["symbol"].dropna().tolist()

        if not sp500_names or not sp500_symbols:
            return None

        match_result = process.extractOne(spy_name, sp500_names, scorer=fuzz.WRatio)

        if match_result:
            _, score, idx = match_result
            if score >= 85:
                self.log.debug(
                    f"Matched '{spy_name}' -> '{sp500_symbols[idx]}' (score: {score})"
                )
                return sp500_symbols[idx]
            else:
                self.log.debug(f"No good match for '{spy_name}' (best score: {score})")
                return None
        else:
            return None

    def save_gold(self, df: pd.DataFrame, execution_date: datetime):
        """Save the GOLD dataset to S3."""
        date_path = execution_date.strftime("%Y/%m/%d")
        output_key = f"{self.gold_prefix}/{date_path}/sp500_gold.parquet"

        self.log.info(f"Saving GOLD dataset → s3://{Config.S3_BUCKET}/{output_key}")

        fs = s3fs.S3FileSystem()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f"s3://{Config.S3_BUCKET}/{output_key}", filesystem=fs)

        self.log.info("GOLD dataset successfully written.")

    def run(self):
        """Run the GOLD enrichment process."""
        execution_date = datetime.utcnow()
        date_path = execution_date.strftime("%Y/%m/%d")

        self.log.info("===== GOLD LOAD: SP500 =====")
        self.log.info(f"Execution date path: {date_path}")

        sp500_df = self.load_sp500_silver(date_path)
        spy_df = self.load_spy_silver(date_path)

        gold_df = self.enrich(sp500_df, spy_df)
        self.save_gold(gold_df, execution_date)

        self.log.info("===== GOLD LOAD COMPLETED SUCCESSFULLY =====")


def main():
    """Run the SP500 GOLD Loader."""
    SP500GoldLoader().run()


if __name__ == "__main__":
    main()
