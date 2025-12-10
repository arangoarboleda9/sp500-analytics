import datetime
from io import BytesIO

import boto3
import pandas as pd
import streamlit as st
from config import Config

s3_client = boto3.client(
    "s3",
    aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
    region_name=Config.AWS_DEFAULT_REGION,
)

bucket_name = Config.AWS_S3_BUCKET_NAME
file_key = Config.AWS_S3_FILE_KEY_GOLD


def load_s3_file(bucket_name, file_key):
    """Load S3 file."""
    today = datetime.datetime.now()
    file_key = (
        f"gold/sp500_daily_prices/{today.strftime('%Y/%m/%d')}/sp500_gold.parquet"
    )
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

    file_content = response["Body"].read()
    df = pd.read_parquet(BytesIO(file_content))

    return df


df = load_s3_file(bucket_name, file_key)

st.title("Vista Gold desde AWS S3")
st.write(df)
