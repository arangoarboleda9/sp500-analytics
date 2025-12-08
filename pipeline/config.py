"""Configuration module for pipeline settings."""
import os

NOT_SET = "not_set"

AWS_DB_USER = os.getenv("AWS_DB_USER", NOT_SET)
AWS_DB_PASSWORD = os.getenv("AWS_DB_PASSWORD", NOT_SET)
AWS_DB_HOST = os.getenv("AWS_DB_HOST", NOT_SET)
AWS_DB_PORT = int(os.getenv("AWS_DB_PORT", 5432))
AWS_DB_NAME = os.getenv("AWS_DB_NAME", NOT_SET)
AWS_DB_URL = (
    f"postgresql://{AWS_DB_USER}:{AWS_DB_PASSWORD}@{AWS_DB_HOST}:5432/{AWS_DB_NAME}"
)


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", NOT_SET)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", NOT_SET)
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", NOT_SET)
S3_BUCKET = os.getenv("S3_BUCKET", NOT_SET)
SP500_URL = os.getenv("SP500_URL", NOT_SET)

API_BASE_URL = os.getenv("API_BASE_URL", NOT_SET)
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")


class Config:
    """Configuration class for pipeline settings."""

    API_BASE_URL = API_BASE_URL
    ENVIRONMENT = ENVIRONMENT

    AWS_DB_USER = AWS_DB_USER
    AWS_DB_PASSWORD = AWS_DB_PASSWORD
    AWS_DB_HOST = AWS_DB_HOST
    AWS_DB_PORT = AWS_DB_PORT
    AWS_DB_NAME = AWS_DB_NAME
    AWS_DB_URL = AWS_DB_URL

    AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION = AWS_DEFAULT_REGION

    S3_BUCKET = S3_BUCKET
    S3_PREFIX = "datasets-kaggle"

    SP500_URL = SP500_URL
    S3_BRONZE_DIR_PREFIX = "bronze"
    S3_SILVER_DIR_PREFIX = "silver"
    S3_BRONZE_PREFIX_SPY = "spy_holdings_raw"
    S3_BRONZE_PREFIX_SP500 = "top_10_sp500_raw"
    S3_SILVER_PREFIX_SP500 = "top_10_sp500_silver"
    S3_SILVER_PREFIX_SPY = "spy_holdings_silver"

    S3_GOLD_DIR_PREFIX = "gold"
    S3_GOLD_PREFIX_SP500_DAILY_PRICES = "sp500_daily_prices"
