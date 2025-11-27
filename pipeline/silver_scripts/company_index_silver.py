import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import boto3
from io import BytesIO
# Aseguramos la importación del módulo config
import config
from config import Config 

# Cargar variables de entorno (para inicializar el módulo config)
load_dotenv()

# -----------------------------------------------------
# ⚙️ CONFIGURACIÓN GLOBAL (Consistente con el módulo config)
# -----------------------------------------------------

# Credenciales AWS/S3 (Inyectadas directamente desde el módulo config)
# Esto asegura que el script dependa de la centralización del módulo config.
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_DEFAULT_REGION
SILVER_BUCKET_NAME = config.S3_BUCKET 
S3_KEY_PATH = "silver/sp500_index/sp500_index_silver.parquet"

# Credenciales de RDS (Inyectadas directamente desde el módulo config)
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# Nombres de las tablas
RAW_TABLE_NAME = "company_index"
SILVER_TABLE_NAME = "sp500_index_silver"


def get_db_engine():
    """Configura y retorna el objeto Engine para la conexión a RDS PostgreSQL."""
    try:
        # Usamos las variables inyectadas por el módulo config
        if DB_HOST == config.NOT_SET:
            raise EnvironmentError("Las variables de entorno de RDS no están configuradas correctamente.")
            
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        print("✔ Conexión a la base de datos RDS establecida.")
        return engine
    except Exception as e:
        print(f"❌ Error al conectar a RDS: {e}")
        return None


def extract_data(engine):
    """Extrae datos de la capa Raw (RDS)."""
    print(f"\nPaso A: Extrayendo datos de la tabla Raw: {RAW_TABLE_NAME}")

    query = f"SELECT * FROM {RAW_TABLE_NAME};"

    try:
        with engine.connect() as connection:
            df_silver = pd.read_sql(query, connection)
        print(f"✔ Datos extraídos. Filas: {len(df_silver)}")
        return df_silver
    except Exception as e:
        print(f"❌ Error al extraer datos de RDS: {e}")
        return pd.DataFrame()


def transform_data(df_raw):
    """Aplica transformaciones, limpieza y estandarización."""
    if df_raw.empty:
        print("DataFrame vacío, omitiendo transformación.")
        return df_raw
        
    df_silver = df_raw.copy()
    print("\nPaso B: Aplicando transformaciones y limpieza...")

    # 1. Renombrar columna problemática (CORRECCIÓN APLICADA)
    df_silver = df_silver.rename(columns={'S&P 500': 'sp500_index', 'S&P500': 'sp500_index'}, errors='ignore')

    # 2. Limpiar y estandarizar todos los nombres de columnas a snake_case
    df_silver.columns = [
        col.lower().replace(' ', '_').replace('.', '_').replace('-', '_').replace('&', '') 
        for col in df_silver.columns
    ]

    # 3. Conversión de Tipos de Datos
    numeric_cols = ['sp500_index', 'dividend', 'earnings']
    for col in numeric_cols:
        if col in df_silver.columns:
            df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')

    # Conversión de fechas
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

    print(f"✔ DataFrame Silver listo. Filas después de la limpieza: {len(df_silver)}")
    return df_silver


def load_to_s3(df_silver):
    """Carga el DataFrame transformado a la capa Silver de S3."""
    if df_silver.empty:
        print("DataFrame vacío, omitiendo carga a S3.")
        return

    print(f"\nPaso C: Cargando datos limpios en la capa Silver de S3...")

    try:
        # 1. Configurar cliente S3 usando las credenciales del módulo config
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            # Se usan las variables del módulo config
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


def load_to_rds(df_silver, engine):
    """Carga el DataFrame Silver al Data Warehouse (RDS)."""
    if df_silver.empty or engine is None:
        print("DataFrame o motor de DB no válidos, omitiendo carga a RDS.")
        return

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


def run_etl_pipeline():
    """Orquesta la ejecución completa del pipeline ETL."""
    
    # 1. Obtener motor de DB
    engine = get_db_engine()
    if engine is None:
        return

    # 2. Extracción
    df_raw = extract_data(engine)
    if df_raw.empty:
        print("Pipeline finalizado sin datos para procesar.")
        return

    # 3. Transformación
    df_silver = transform_data(df_raw)
    
    if df_silver.empty:
        print("Pipeline finalizado, todos los datos fueron descartados en la limpieza.")
        return

    # 4. Carga a S3
    load_to_s3(df_silver)

    # 5. Carga a RDS
    load_to_rds(df_silver, engine)


if __name__ == "__main__":
    run_etl_pipeline()