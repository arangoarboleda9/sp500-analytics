import logging
from config import Config
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB


def connect_to_rds() -> any:
    """Creates a SQLAlchemy connection to the RDS PostgreSQL."""
    db_url = Config.AWS_DB_URL
    logging.info(f"Connecting to RDS: db_url={db_url}")
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
        conn.close()
        logging.info("Connected to RDS successfully.")
    except Exception as e:
        logging.error(f"Failed to connect to RDS: {e}")
        raise
    return engine


def insert_jsonb_table(df: pd.DataFrame, table_name: str, engine):
    """Inserts a DataFrame into a PostgreSQL table with a JSONB column."""
    logging.info("Transformando filas a formato JSONB...")
    df_json = df.apply(lambda row: row.to_dict(), axis=1).to_frame(name="data")

    logging.info(f"Inserting data into table '{table_name}'...")

    df_json.to_sql(
        table_name, engine, if_exists="replace", index=False, dtype={"data": JSONB}
    )

    logging.info(f"Table '{table_name}' created and imported successfully.")


def insert_table(df: pd.DataFrame, table_name: str, engine):
    """Inserts a DataFrame directly into a PostgreSQL table."""

    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info(f"Inserting data into table '{table_name}'...")

    except Exception as e:
        logging.error(f"Failed to insert data into RDS: {e}")
        raise
    logging.info(f"Table '{table_name}' created and imported successfully.")