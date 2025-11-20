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

# NUEVO ARCHIVO A TRAER DESDE S3
AWS_KEY = "datasets-kaggle/dixitdatascientist_s-and-p-500-esg-risk-analysis/SP500_ESG_Cleaned.csv"

# Credenciales de RDS desde .env
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Nueva tabla a crear en RDS
TABLE_NAME = "company_risk_analysis"

# -----------------------------------------------------
# Descargar archivo desde S3 usando boto3
# -----------------------------------------------------
print(f"Descargando archivo {AWS_KEY} desde S3...")

s3 = boto3.client("s3", region_name=AWS_REGION)

response = s3.get_object(Bucket=AWS_BUCKET, Key=AWS_KEY)
csv_data = response["Body"].read().decode("utf-8")

# Cargar datos en DataFrame
df = pd.read_csv(StringIO(csv_data))

print("CSV descargado correctamente. Primeras filas:")
print(df.head())

# -----------------------------------------------------
# Conectar a RDS PostgreSQL
# -----------------------------------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Crear tabla e insertar datos
# -----------------------------------------------------
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

print(f"✔ Tabla '{TABLE_NAME}' creada e importada exitosamente.")
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

# NUEVO ARCHIVO A TRAER DESDE S3
AWS_KEY = "datasets-kaggle/dixitdatascientist_s-and-p-500-esg-risk-analysis/SP500_ESG_Cleaned.csv"

# Credenciales de RDS desde .env
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Nueva tabla a crear en RDS
TABLE_NAME = "company_risk_analysis"

# -----------------------------------------------------
# Descargar archivo desde S3 usando boto3
# -----------------------------------------------------
print(f"Descargando archivo {AWS_KEY} desde S3...")

s3 = boto3.client("s3", region_name=AWS_REGION)

response = s3.get_object(Bucket=AWS_BUCKET, Key=AWS_KEY)
csv_data = response["Body"].read().decode("utf-8")

# Cargar datos en DataFrame
df = pd.read_csv(StringIO(csv_data))

print("CSV descargado correctamente. Primeras filas:")
print(df.head())

# -----------------------------------------------------
# Conectar a RDS PostgreSQL
# -----------------------------------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Crear tabla e insertar datos
# -----------------------------------------------------
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

print(f"✔ Tabla '{TABLE_NAME}' creada e importada exitosamente.")
