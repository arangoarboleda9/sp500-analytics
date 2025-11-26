import logging
from config import Config
from utils.aws_utils import download_csv_from_s3
from utils.db_utils import connect_to_rds, insert_jsonb_table, insert_table


class BaseRawLoader:
    def __init__(self, table_name, s3_key):
        self.table_name = table_name
        self.s3_key = s3_key
        self.bucket = Config.S3_BUCKET
        self.region = Config.AWS_DEFAULT_REGION

    def load_csv(self):
        logging.info(f"Loading CSV for table: {self.table_name}")
        return download_csv_from_s3(self.bucket, self.s3_key, self.region)

    def connect(self):
        return connect_to_rds()

    def persist(self, df, engine):
        """To be implemented by subclasses for specific persistence logic."""
        raise NotImplementedError

    def run(self):
        logging.info(f"\n===== RAW TABLE: {self.table_name} =====")

        df = self.load_csv()
        engine = self.connect()
        self.persist(df, engine)

        logging.info(f"===== END RAW TABLE: {self.table_name} =====\n")


class RawTableLoader(BaseRawLoader):
    def persist(self, df, engine):
        insert_table(df, self.table_name, engine)


class RawJsonbLoader(BaseRawLoader):
    def persist(self, df, engine):
        insert_jsonb_table(df, self.table_name, engine)
