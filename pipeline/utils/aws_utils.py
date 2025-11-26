import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from io import StringIO


def download_csv_from_s3(bucket: str, key: str, region: str) -> pd.DataFrame:
    """Downloads a CSV file from S3 and returns it as a DataFrame."""
    logging.info(f"Downloading file from S3: s3://{bucket}/{key}")
    try:
        s3 = boto3.client("s3", region_name=region)
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = response["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"Error downloading file from S3: {e}")
        raise

    df = pd.read_csv(StringIO(csv_data), low_memory=False)
    logging.info("CSV downloaded successfully. First rows:")
    logging.info(df.head())

    return df
