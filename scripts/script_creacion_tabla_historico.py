import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
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

# NUEVO ARCHIVO A TRAER DESDE S3
AWS_KEY = "datasets-kaggle/chickenrobot_historical-stocks-of-companies-of-the-sp-and-500/Sp500_historical.csv"

# Credenciales de RDS desde .env
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

TABLE_NAME = "company_historical_stocks"

# -----------------------------------------------------
# Descargar archivo desde S3 usando boto3
# -----------------------------------------------------
print(f"Descargando archivo {AWS_KEY} desde S3...")

s3 = boto3.client("s3", region_name=AWS_REGION)
response = s3.get_object(Bucket=AWS_BUCKET, Key=AWS_KEY)
csv_data = response["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(csv_data), low_memory=False)

print("CSV descargado correctamente. Primeras filas:")
print(df.head())

# -----------------------------------------------------
# Convertir cada fila en JSON
# -----------------------------------------------------
df_json = df.apply(lambda row: row.to_dict(), axis=1).to_frame(name="data")

# -----------------------------------------------------
# Conectar a RDS PostgreSQL
# -----------------------------------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Crear tabla e insertar datos usando JSONB
# -----------------------------------------------------
df_json.to_sql(
    TABLE_NAME,
    engine,
    if_exists="replace",
    index=False,
    dtype={"data": JSONB}
)

print(f"✔ Tabla '{TABLE_NAME}' creada e importada exitosamente con formato JSONB.")
