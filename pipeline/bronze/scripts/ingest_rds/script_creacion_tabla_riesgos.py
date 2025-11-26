from config import Config
from raw_loader import RawTableLoader

AWS_KEY = f"{Config.S3_PREFIX}/dixitdatascientist_s-and-p-500-esg-risk-analysis/SP500_ESG_Cleaned.csv"
TABLE_NAME = "company_risk_analysis_test"


def main():
    loader = RawTableLoader(table_name=TABLE_NAME, s3_key=AWS_KEY)
    loader.run()


if __name__ == "__main__":
    main()
