"""
Ingesta a RDS:
1. Descarga un CSV desde S3 (datasets-kaggle)
2. Carga el DataFrame directamente en PostgreSQL
3. Estructurado con main() para ejecución desde Airflow
"""

from config import Config
from raw_loader import RawTableLoader

AWS_KEY = f"{Config.S3_PREFIX}/vaghefi_company-reviews/company_reviews.csv"
TABLE_NAME = "company_reviews"


def main():
    loader = RawTableLoader(table_name=TABLE_NAME, s3_key=AWS_KEY)
    loader.run()


if __name__ == "__main__":
    main()
