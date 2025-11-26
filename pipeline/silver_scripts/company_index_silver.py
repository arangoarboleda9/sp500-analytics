import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import numpy as np
import boto3
# Importar BytesIO, el buffer correcto para formatos binarios como Parquet
from io import BytesIO
from config import Config
import config

# -----------------------------------------------------
# ⚙️ CONFIGURACIÓN DE CREDENCIALES AWS y DESTINO S3
# -----------------------------------------------------

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = config.AWS_DEFAULT_REGION
SILVER_BUCKET_NAME = config.S3_BUCKET 
S3_KEY_PATH = "silver/sp500_index/sp500_index_silver.parquet"

# -----------------------------------------------------
# 🗄️ CONFIGURACIÓN DE RDS Y TABLAS (Usando la clase Config)
# -----------------------------------------------------
# Credenciales de RDS (Usando la clase Config)
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# Nombres de las tablas
RAW_TABLE_NAME = "company_index"
SILVER_TABLE_NAME = "sp500_index_silver"

# -----------------------------------------------------
# Conectar a RDS PostgreSQL

db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Paso A: Extracción (E) - Leer datos desde la capa Raw (RDS)
# -----------------------------------------------------
print(f"Paso A: Extrayendo datos de la tabla Raw: {RAW_TABLE_NAME}")

query = f"SELECT * FROM {RAW_TABLE_NAME};"

try:
    with engine.connect() as connection:
        df_silver = pd.read_sql(query, connection)
except Exception as e:
    print(f"❌ Error al extraer datos de RDS: {e}")
    exit()

print(f"Datos extraídos. Filas: {len(df_silver)}")

# -----------------------------------------------------
# Paso B: Transformación (T) - Limpieza y Ajustes
# -----------------------------------------------------
print("Paso B: Aplicando transformaciones y limpieza...")

# 1. Renombrar columna problemática antes de la limpieza general (CORRECCIÓN APLICADA)
df_silver = df_silver.rename(columns={'S&P 500': 'sp500_index', 'S&P500': 'sp500_index'}, errors='ignore')

# 2. Limpiar y estandarizar todos los nombres de columnas a minúsculas y snake_case
df_silver.columns = [col.lower().replace(' ', '_').replace('.', '_').replace('-', '_').replace('&', '') for col in df_silver.columns]

# 3. Conversión de Tipos de Datos
numeric_cols = ['sp500_index', 'dividend', 'earnings']
for col in numeric_cols:
    if col in df_silver.columns:
        df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')

# Conversión de fechas (para 'Date')
if 'date' in df_silver.columns:
    df_silver['date'] = pd.to_datetime(df_silver['date'], errors='coerce')

# 4. Limpieza: Eliminar filas con fechas nulas o índice S&P nulo
df_silver.dropna(subset=['date', 'sp500_index'], inplace=True)

# 5. Selección final de columnas y orden
final_columns = [
    'date',
    'sp500_index',
    'dividend',
    'earnings'
]
df_silver = df_silver[[col for col in final_columns if col in df_silver.columns]]

print(f"DataFrame Silver listo. Filas después de la limpieza: {len(df_silver)}")
print("Tipos de datos finales:")
print(df_silver.dtypes)

# -----------------------------------------------------
# Paso C: Carga a S3 (Data Lake - Carga Principal)
# -----------------------------------------------------
print(f"\nPaso C: Cargando datos limpios en la capa Silver de S3...")

try:
    # 1. Configurar cliente S3 con credenciales
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        # Ahora se cargan del entorno y NO están hardcodeadas en el código
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # 2. Convertir DataFrame a Parquet usando BytesIO
    parquet_buffer = BytesIO()
    df_silver.to_parquet(parquet_buffer, index=False)

    parquet_buffer.seek(0)

    # 3. Subir el archivo binario a S3
    s3_client.put_object(
        Bucket=SILVER_BUCKET_NAME,
        Key=S3_KEY_PATH,
        Body=parquet_buffer.read()
    )

    print(f"✔ Carga en S3 exitosa: s3://{SILVER_BUCKET_NAME}/{S3_KEY_PATH}")

except Exception as e:
    print(f"❌ Error al cargar a S3: {e}")


# -----------------------------------------------------
# Paso D: Carga a RDS (Data Warehouse/Análisis - Carga Secundaria)
# -----------------------------------------------------
print(f"\nPaso D: Cargando datos Silver en la tabla RDS: {SILVER_TABLE_NAME}")

try:
    df_silver.to_sql(
        SILVER_TABLE_NAME,
        engine,
        if_exists="replace", # Reemplaza la tabla si ya existe
        index=False
    )
    print(f"✔ Tabla Silver '{SILVER_TABLE_NAME}' creada y cargada exitosamente en RDS.")
except Exception as e:
    print(f"❌ Error al cargar a RDS: {e}")