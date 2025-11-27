"""
Ingesta a RDS:
1. Descarga un CSV desde S3 (datasets-kaggle)
2. Lo transforma en formato JSONB fila por fila
3. Lo inserta en una tabla PostgreSQL en RDS
"""

from config import Config
from raw_loader import RawJsonbLoader


TABLE_NAME = "company_historical_stocks"
AWS_KEY = (
    f"{Config.S3_PREFIX}/"
    "chickenrobot_historical-stocks-of-companies-of-the-sp-and-500/"
    "Sp500_historical.csv"
)


def main():
    loader = RawJsonbLoader(
        table_name=TABLE_NAME,
        s3_key=AWS_KEY
    )
    loader.run()


if __name__ == "__main__":
    main()
