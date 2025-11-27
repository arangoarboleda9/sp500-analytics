from config import Config
from raw_loader import RawTableLoader


AWS_KEY = f"{Config.S3_PREFIX}/andrewmvd_sp-500-stocks/sp500_index.csv"
TABLE_NAME = "company_index"


def main():
    loader = RawTableLoader(
        table_name=TABLE_NAME,
        s3_key=AWS_KEY
    )
    loader.run()


if __name__ == "__main__":
    main()
