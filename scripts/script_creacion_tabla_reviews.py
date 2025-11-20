import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import boto3
import os
from io import StringIO

# -----------------------------------------------------
# Cargar variables desde .env
# -----------------------------------------------------
load_dotenv()

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_BUCKET = "henry-sp500-datasets"
AWS_KEY = "datasets-kaggle/vaghefi_company-reviews/company_reviews.csv"

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

TABLE_NAME = "company_reviews"

# -----------------------------------------------------
# Descargar archivo desde S3 usando boto3
# -----------------------------------------------------
print("Descargando CSV desde S3 usando boto3...")

s3 = boto3.client("s3", region_name=AWS_REGION)

response = s3.get_object(Bucket=AWS_BUCKET, Key=AWS_KEY)

csv_data = response["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(csv_data))

print("CSV descargado correctamente. Primeras filas:")
print(df.head())

# -----------------------------------------------------
# Conectar a PostgreSQL RDS
# -----------------------------------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Crear tabla e insertar datos
# -----------------------------------------------------
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

print(f"✔ Tabla '{TABLE_NAME}' creada e importada exitosamente.")
