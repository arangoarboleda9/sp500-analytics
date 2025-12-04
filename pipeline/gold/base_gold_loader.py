"""Base class for GOLD layer loaders."""
from datetime import datetime

import boto3
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.aws_utils import S3Utils


class BaseGoldLoader(LoggingMixin):
    """
    Base class for GOLD layer loaders.

    Handles reading Silver, enriching data, and saving Gold output.
    """

    def __init__(self, bucket: str, silver_prefix: str, gold_prefix: str):
        """Initialize Base Gold Loader with S3 bucket and prefixes."""
        super().__init__()
        self.bucket = bucket
        self.silver_prefix = silver_prefix
        self.gold_prefix = gold_prefix
        self.s3 = boto3.client("s3")
        self.utils = S3Utils()

    def load_latest_silver(self, date_path: str) -> pd.DataFrame:
        """Load the latest Silver Parquet file from S3 for the given date path."""
        silver_path = f"{self.silver_prefix}/{date_path}"
        self.log.info(f"Searching Silver files at: s3://{self.bucket}/{silver_path}")

        keys = self.utils.list_keys(self.bucket, silver_path)
        if not keys:
            raise FileNotFoundError(f"No Silver files found in {silver_path}")

        latest_key = sorted(keys)[-1]
        self.log.info(f"Loading Silver file: s3://{self.bucket}/{latest_key}")

        return self.utils.download_csv_to_df(self.bucket, latest_key)

    def save_gold(self, df: pd.DataFrame, date_path: str):
        """Save the Gold DataFrame to S3 as Parquet with date path."""
        ts = datetime.utcnow().strftime("%H%M%S")
        filename = f"sp500_daily_prices_{ts}.parquet"

        s3_key = f"{self.gold_prefix}/{date_path}/{filename}"

        self.log.info(f"Saving GOLD parquet: s3://{self.bucket}/{s3_key}")

        df.to_parquet(f"s3://{self.bucket}/{s3_key}", index=False)

    def enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the DataFrame with additional data. To be implemented in subclass."""
        raise NotImplementedError("Child classes must implement enrich_data().")

    def run(self):
        """Run the Gold Loader process."""
        self.log.info(f"===== GOLD LOAD: {self.__class__.__name__} =====")

        now = datetime.utcnow()
        date_path = now.strftime("%Y/%m/%d")

        silver_df = self.load_latest_silver(date_path)
        gold_df = self.enrich_data(silver_df)
        self.save_gold(gold_df, date_path)

        self.log.info("===== END GOLD LOAD =====")
