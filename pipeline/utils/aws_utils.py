"""AWS S3 Utility Functions and S3Utils class (supports CSV + Parquet)."""

import io
import logging
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from config import Config


def download_csv_from_s3(bucket: str, key: str, region: str) -> pd.DataFrame:
    """Download a CSV file from S3 and return it as a DataFrame."""
    logging.info(f"Downloading CSV from S3: s3://{bucket}/{key}")
    try:
        s3 = boto3.client("s3", region_name=region)
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = response["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"Error downloading CSV from S3: {e}")
        raise

    return pd.read_csv(StringIO(csv_data), low_memory=False)


def list_s3_keys(
    bucket: str, prefix: str, region: str = Config.AWS_DEFAULT_REGION
) -> list:
    """List all keys under a prefix."""
    logging.info(f"Listing keys: s3://{bucket}/{prefix}")
    s3 = boto3.client("s3", region_name=region)
    keys = []
    paginator = s3.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except ClientError as e:
        logging.error(f"Error listing keys: {e}")
        raise

    logging.info(f"Found {len(keys)} keys under prefix '{prefix}'")
    return keys


def download_s3_object(
    bucket: str, key: str, region: str = Config.AWS_DEFAULT_REGION
) -> StringIO:
    """Download a text-based object from S3 and return StringIO."""
    logging.info(f"Downloading object from S3: s3://{bucket}/{key}")

    try:
        s3 = boto3.client("s3", region_name=region)
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"Error downloading object: {e}")
        raise

    return StringIO(content)


# ============================================================
#  CLASS-BASED VERSION OF S3 UTILITIES
# ============================================================


class S3Utils:
    """Modern S3 utility wrapper for CSV and Parquet files."""

    def __init__(self, region: str = Config.AWS_DEFAULT_REGION):
        self.region = Config.AWS_DEFAULT_REGION
        self.s3 = boto3.client("s3", region_name=self.region)

    def list_keys(self, bucket: str, prefix: str):
        """List all keys starting with prefix."""
        logging.info(f"Listing keys under: s3://{bucket}/{prefix}")
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        logging.info(f"Found {len(keys)} keys.")
        return keys

    def get_latest_key(self, bucket: str, prefix: str) -> str | None:
        """Find the latest modified key under a prefix."""
        keys_info = []

        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys_info.append((obj["Key"], obj["LastModified"]))

        if not keys_info:
            return None

        # Ordenar por fecha descendente
        keys_info.sort(key=lambda x: x[1], reverse=True)

        # Obtener el key más reciente (posición 0, primer elemento de la tupla)
        latest_key = keys_info[0][0]

        return latest_key

    def read_csv(self, bucket: str, key: str) -> pd.DataFrame:
        """Read a CSV file (UTF-8)."""
        logging.info(f"Reading CSV: s3://{bucket}/{key}")
        response = self.s3.get_object(Bucket=bucket, Key=key)

        data = response["Body"].read().decode("utf-8")
        return pd.read_csv(StringIO(data))

    def read_parquet(self, bucket: str, key: str) -> pd.DataFrame:
        """Read a Parquet file from S3/MinIO."""
        logging.info(f"Reading Parquet: s3://{bucket}/{key}")
        response = self.s3.get_object(Bucket=bucket, Key=key)

        raw_bytes = response["Body"].read()
        return pd.read_parquet(io.BytesIO(raw_bytes))

    def write_parquet(self, df: pd.DataFrame, bucket: str, key: str):
        """Write a DataFrame as Parquet to S3."""
        logging.info(f"Writing Parquet → s3://{bucket}/{key}")

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

        logging.info("Parquet written successfully.")
