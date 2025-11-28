import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import numpy as np
import boto3
from io import BytesIO
import config
from config import Config
from typing import Tuple, Dict, Any

# Cargar variables de entorno
load_dotenv()

# -----------------------------------------------------
# ⚠️ CONFIGURACIÓN DE CREDENCIALES AWS y DESTINO S3 ⚠️
# -----------------------------------------------------v

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_DEFAULT_REGION
SILVER_BUCKET_NAME = config.S3_BUCKET
# ✅ Path Correcto para info
S3_KEY_PATH = "silver/company_info/fact_company_info.parquet"

# -----------------------------------------------------\
# CONFIGURACIÓN DE RDS Y TABLAS
# -----------------------------------------------------\
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME
# Nombres de las tablas
RAW_TABLE_NAME = "company_info"
FACT_TABLE_NAME = "fact_company_info"
DIM_SECTOR_NAME = "dim_sector"
DIM_INDUSTRY_NAME = "dim_industry"
DIM_LOCATION_NAME = "dim_location"
DIM_EXCHANGE_NAME = "dim_exchange"


def get_db_engine():
    """Configura y retorna el objeto Engine para la conexión a RDS PostgreSQL."""
    try:
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
    """Extrae datos de la capa Raw."""
    print(f"\nPaso A: Extrayendo datos de la tabla Raw: {RAW_TABLE_NAME}")
    
    query = f"SELECT * FROM {RAW_TABLE_NAME};"
    try:
        with engine.connect() as connection:
            df_raw = pd.read_sql(query, connection)
        
        print(f"✔ Datos extraídos. Filas: {len(df_raw)}")
        return df_raw
    except Exception as e:
        print(f"❌ Error al extraer datos de RDS: {e}")
        return pd.DataFrame()


def transform_data(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aplica limpieza, estandarización y normalización (creación de dimensiones)."""
    
    if df_raw.empty:
        print("DataFrame de entrada vacío, omitiendo transformación.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    df = df_raw.copy()
    print("\nPaso B: Aplicando transformaciones y normalización...")

    # 1. Limpieza de nombres de columnas
    df.columns = [col.lower().replace(' ', '_').replace('.', '').replace('-', '_') for col in df.columns]

    # 2. Renombrar y crear ID Único (Primary Key)
    # Renombrar el Ticker ('Company Ticker' -> 'company_ticker' -> 'symbol')
    df = df.rename(columns={'company_ticker': 'symbol', 'id': 'company_id'}, errors='ignore')
    
    # Si la columna 'company_id' no existe o está vacía (por si el 'id' raw no está), creamos un ID secuencial.
    if 'company_id' not in df.columns or df['company_id'].isnull().all():
        df['company_id'] = df.reset_index().index + 1
        print("Advertencia: 'company_id' creado secuencialmente.")
    
    # 3. Limpieza y estandarización de columnas
    
    # Columnas de texto para dimensiones (Nombres después de la limpieza)
    text_dim_cols = ['sector', 'industry', 'location', 'exchange']
    
    for col in text_dim_cols:
        if col in df.columns:
            # Rellenar nulos/vacíos para evitar errores en la creación de dimensiones
            df[col] = df[col].astype(str).str.strip().replace('nan', 'Unknown', regex=False).str.title()

    # Columnas numéricas (No son ratings, sino datos de la empresa)
    numeric_cols = ['employees', 'founded']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # --- PASO DE NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

    # 1. DIM_SECTOR
    df_dim_sector = df[['sector']].drop_duplicates().reset_index(drop=True)
    df_dim_sector['sector_id'] = df_dim_sector.index + 1
    df_dim_sector = df_dim_sector[['sector_id', 'sector']]
    df = pd.merge(df, df_dim_sector, on='sector', how='left')

    # 2. DIM_INDUSTRY
    df_dim_industry = df[['industry']].drop_duplicates().reset_index(drop=True)
    df_dim_industry['industry_id'] = df_dim_industry.index + 1
    df_dim_industry = df_dim_industry[['industry_id', 'industry']]
    df = pd.merge(df, df_dim_industry, on='industry', how='left')

    # 3. DIM_LOCATION
    df_dim_location = df[['location']].drop_duplicates().reset_index(drop=True)
    df_dim_location['location_id'] = df_dim_location.index + 1
    df_dim_location = df_dim_location[['location_id', 'location']]
    df = pd.merge(df, df_dim_location, on='location', how='left')

    # 4. DIM_EXCHANGE
    df_dim_exchange = df[['exchange']].drop_duplicates().reset_index(drop=True)
    df_dim_exchange['exchange_id'] = df_dim_exchange.index + 1
    df_dim_exchange = df_dim_exchange[['exchange_id', 'exchange']]
    df = pd.merge(df, df_dim_exchange, on='exchange', how='left')

    # 5. CREACIÓN DE LA TABLA DE HECHOS (FACT_COMPANY_INFO)
    
    # ✅ SELECCIÓN FINAL DE COLUMNAS CORRECTAS PARA LA TABLA DE HECHOS (INFO)
    # Nota: Excluimos ratings como 'overall_rating', 'work_life_balance', etc.
    fact_columns = [
        'company_id', # Primary Key
        'symbol',     # Ticker de la empresa
        'company_name',
        'ceo',
        'founded',
        'employees',
        'description',
        'isin',
        # Foreign Keys
        'sector_id',
        'industry_id',
        'location_id',
        'exchange_id'
    ]

    df_fact_table = df[[col for col in fact_columns if col in df.columns]]
    print(f"✔ Normalización completa. DataFrame de Hechos (INFO) listo. Filas: {len(df_fact_table)}")
    
    return df_fact_table, df_dim_sector, df_dim_industry, df_dim_location, df_dim_exchange


def load_to_s3(df_fact_table: pd.DataFrame):
    """Carga la Tabla de Hechos a la capa Silver de S3."""
    if df_fact_table.empty:
        print("DataFrame de hechos vacío, omitiendo carga a S3.")
        return

    print(f"\nPaso C: Cargando la Tabla de Hechos en la capa Silver de S3...")

    try:
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        parquet_buffer = BytesIO()
        df_fact_table.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        s3_client.put_object(
            Bucket=SILVER_BUCKET_NAME,
            Key=S3_KEY_PATH,
            Body=parquet_buffer.read()
        )

        print(f"✔ Carga en S3 exitosa: s3://{SILVER_BUCKET_NAME}/{S3_KEY_PATH}")

    except Exception as e:
        print(f"❌ Error al cargar a S3: {e}")


def load_to_rds(engine, df_fact_table: pd.DataFrame, df_dim_sector: pd.DataFrame, df_dim_industry: pd.DataFrame, df_dim_location: pd.DataFrame, df_dim_exchange: pd.DataFrame):
    """Carga Tablas de Dimensión y Hechos a RDS y establece las claves."""
    if df_fact_table.empty or engine is None:
        print("DataFrame o motor de DB no válidos, omitiendo carga a RDS.")
        return

    print(f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella")

    tables_to_load = [
        (df_dim_sector, DIM_SECTOR_NAME),
        (df_dim_industry, DIM_INDUSTRY_NAME),
        (df_dim_location, DIM_LOCATION_NAME),
        (df_dim_exchange, DIM_EXCHANGE_NAME),
        (df_fact_table, FACT_TABLE_NAME),
    ]

    try:
        with engine.begin() as connection:
            # 1. Cargar todas las tablas (Dimensión y Hechos)
            for df, table_name in tables_to_load:
                 df.to_sql(table_name, connection, if_exists="replace", index=False)
                 print(f"✔ Tabla '{table_name}' cargada.")
            
            # 2. Establecer PRIMARY KEYs y FOREIGN KEYs
            print("Estableciendo claves para la integridad referencial...")
            
            # PRIMARY KEYs
            connection.execute(text(f"ALTER TABLE {DIM_SECTOR_NAME} ADD PRIMARY KEY (sector_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_INDUSTRY_NAME} ADD PRIMARY KEY (industry_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_LOCATION_NAME} ADD PRIMARY KEY (location_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_EXCHANGE_NAME} ADD PRIMARY KEY (exchange_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD PRIMARY KEY (company_id);"))

            # FOREIGN KEYs (FKs)
            connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (sector_id) REFERENCES {DIM_SECTOR_NAME} (sector_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (industry_id) REFERENCES {DIM_INDUSTRY_NAME} (industry_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (location_id) REFERENCES {DIM_LOCATION_NAME} (location_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (exchange_id) REFERENCES {DIM_EXCHANGE_NAME} (exchange_id);"))

        print("🎉 Carga a RDS completada con Esquema Estrella para Company Info.")

    except Exception as e:
        print(f"❌ Error al cargar a RDS o al establecer claves: {e}")


def run_etl_pipeline():
    """Orquesta la ejecución completa del pipeline ETL."""
    
    # 1. Obtener motor de DB
    engine = get_db_engine()
    if engine is None:
        return

    # 2. Extracción
    df_raw = extract_data(engine)
    if df_raw.empty:
        print("Pipeline finalizado por falta de datos necesarios.")
        return

    # 3. Transformación y Normalización
    df_fact_table, df_dim_sector, df_dim_industry, df_dim_location, df_dim_exchange = transform_data(df_raw)
    
    if df_fact_table.empty:
        print("Pipeline finalizado, no hay datos válidos después de la limpieza.")
        return

    # 4. Carga a S3
    load_to_s3(df_fact_table)

    # 5. Carga a RDS y Establecimiento de Claves
    load_to_rds(engine, df_fact_table, df_dim_sector, df_dim_industry, df_dim_location, df_dim_exchange)


if __name__ == "__main__":
    run_etl_pipeline()