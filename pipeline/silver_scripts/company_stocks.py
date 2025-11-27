import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import numpy as np 
import boto3
from io import BytesIO 
import config
from config import Config
from typing import Tuple

# Cargar variables de entorno (necesario para que el módulo config las inicialice)
load_dotenv() 

# -----------------------------------------------------
# ⚙️ CONFIGURACIÓN GLOBAL (Sourced desde el módulo config)
# -----------------------------------------------------

# Credenciales AWS/S3 (Inyectadas directamente desde el módulo config)
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID 
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_DEFAULT_REGION         
SILVER_BUCKET_NAME = config.S3_BUCKET    
S3_KEY_PATH = "silver/company_stocks/fact_stock_prices.parquet" 

# Credenciales de RDS (Inyectadas directamente desde el módulo config)
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# Nombres de las tablas
RAW_TABLE_NAME = "company_stocks" 
FACT_STOCKS_NAME = "fact_stock_prices" 
DIM_DATE_NAME = "dim_date"
DIM_SYMBOL_NAME = "dim_symbol"


def get_db_engine():
    """Configura y retorna el objeto Engine para la conexión a RDS PostgreSQL."""
    try:
        # Verifica la configuración de la base de datos
        if DB_HOST == config.NOT_SET or DB_PORT == config.NOT_SET or not str(DB_PORT).isdigit():
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
            df_raw = pd.read_sql(query, connection)
        print(f"✔ Datos extraídos. Filas: {len(df_raw)}")
        return df_raw
    except Exception as e:
        print(f"❌ Error al extraer datos de RDS: {e}")
        return pd.DataFrame()


def transform_data(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aplica limpieza, estandarización de fechas/tickers y la NORMALIZACIÓN."""
    
    if df_raw.empty:
        print("DataFrame vacío, omitiendo transformación.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() 

    print("\nPaso B: Aplicando transformaciones y normalización (Esquema Estrella)...")
    df_fact = df_raw.copy()

    # 1. Limpieza de nombres de columnas a snake_case
    df_fact.columns = [col.lower().replace(' ', '_').replace('.', '').replace('-', '_') for col in df_fact.columns]

    # 2. Definición de Tipos y Limpieza
    numeric_cols = ['adj_close', 'close', 'high', 'low', 'open', 'volume']
    
    for col in numeric_cols:
        if col in df_fact.columns:
            df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce')
            
    # CONVERSIÓN DE FECHAS
    if 'date' in df_fact.columns:
        df_fact['date'] = pd.to_datetime(df_fact['date'], errors='coerce')
        
    # LIMPIEZA DE SYMBOL (Ticker)
    if 'symbol' in df_fact.columns:
        df_fact['symbol'] = df_fact['symbol'].astype(str).str.strip().str.upper()

    # 3. Eliminar filas con información crítica faltante
    df_fact.dropna(subset=['date', 'symbol', 'adj_close'], inplace=True)
    df_fact = df_fact.reset_index(drop=True)

    # --- NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

    # 1. DIM_DATE
    df_dim_date = df_fact[['date']].drop_duplicates().dropna().copy().reset_index(drop=True)
    df_dim_date['date_id'] = df_dim_date.index + 1
    df_dim_date['year'] = df_dim_date['date'].dt.year
    df_dim_date['month'] = df_dim_date['date'].dt.month
    df_dim_date['day'] = df_dim_date['date'].dt.day
    df_dim_date['day_of_week'] = df_dim_date['date'].dt.dayofweek # 0=Lunes, 6=Domingo
    df_dim_date['quarter'] = df_dim_date['date'].dt.quarter
    df_dim_date['is_weekend'] = np.where(df_dim_date['date'].dt.dayofweek.isin([5, 6]), True, False)

    df_dim_date = df_dim_date[['date_id', 'date', 'year', 'month', 'day', 'day_of_week', 'quarter', 'is_weekend']]
    df_fact = pd.merge(df_fact, df_dim_date[['date_id', 'date']], on='date', how='left')


    # 2. DIM_SYMBOL
    df_dim_symbol = df_fact[['symbol']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_symbol['symbol_id'] = df_dim_symbol.index + 1
    df_dim_symbol = df_dim_symbol[['symbol_id', 'symbol']]
    df_fact = pd.merge(df_fact, df_dim_symbol, on='symbol', how='left')


    # 3. CREACIÓN DE LA TABLA DE HECHOS (FACT_STOCK_PRICES)
    df_fact = df_fact.sort_values(by=['symbol', 'date']).reset_index(drop=True)
    df_fact['stock_price_id'] = df_fact.index + 1

    fact_columns = [
        'stock_price_id',  
        'date_id',         
        'symbol_id',       
        'adj_close',       
        'close',           
        'high',            
        'low',             
        'open',            
        'volume'           
    ]

    df_fact_table = df_fact[[col for col in fact_columns if col in df_fact.columns]].copy()
    # Asegurar tipos INT para las claves
    df_fact_table['date_id'] = df_fact_table['date_id'].astype(np.int64)
    df_fact_table['symbol_id'] = df_fact_table['symbol_id'].astype(np.int64)

    print(f"✔ Normalización completa. DataFrame de Hechos listo. Filas: {len(df_fact_table)}")

    # Retorna la tabla de hechos y ambas dimensiones
    return df_fact_table, df_dim_date, df_dim_symbol


def load_to_s3(df_fact_table: pd.DataFrame):
    """Carga la Tabla de Hechos a la capa Silver de S3."""
    if df_fact_table.empty:
        print("DataFrame de hechos vacío, omitiendo carga a S3.")
        return

    print(f"\nPaso C: Cargando la Tabla de Hechos en la capa Silver de S3...")

    try:
        # Configurar cliente S3 con credenciales de config
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        parquet_buffer = BytesIO() 
        df_fact_table.to_parquet(parquet_buffer, index=False)
        
        s3_client.put_object(
            Bucket=SILVER_BUCKET_NAME,
            Key=S3_KEY_PATH,
            Body=parquet_buffer.getvalue() 
        )

        print(f"✔ Carga en S3 exitosa: s3://{SILVER_BUCKET_NAME}/{S3_KEY_PATH}")

    except Exception as e:
        print(f"❌ Error al cargar a S3: {e}")


def load_to_rds(engine, df_fact_table: pd.DataFrame, df_dim_date: pd.DataFrame, df_dim_symbol: pd.DataFrame):
    """Carga Tablas de Dimensión y Hechos a RDS y establece las claves."""
    if df_fact_table.empty or engine is None:
        print("DataFrame o motor de DB no válidos, omitiendo carga a RDS.")
        return

    print(f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella")

    tables_to_load = [
        (df_dim_date, DIM_DATE_NAME),
        (df_dim_symbol, DIM_SYMBOL_NAME),
        (df_fact_table, FACT_STOCKS_NAME), 
    ]

    try:
        with engine.begin() as connection:
            
            # Limpiar tablas existentes (DROP CASCADE)
            print("Limpiando tablas existentes (DROP CASCADE)...")
            connection.execute(text(f"DROP TABLE IF EXISTS {FACT_STOCKS_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_DATE_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_SYMBOL_NAME} CASCADE;"))
            print("Limpieza completada.")

            # 1. Cargar las tablas de Dimensión y la de Hechos
            for df, table_name in tables_to_load:
                 df.to_sql(table_name, connection, if_exists="replace", index=False)
                 print(f"✔ Tabla '{table_name}' cargada.")

            # 2. Establecer PRIMARY KEYs
            print("Estableciendo claves primarias...")
            connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD PRIMARY KEY (stock_price_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_DATE_NAME} ADD PRIMARY KEY (date_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_SYMBOL_NAME} ADD PRIMARY KEY (symbol_id);"))
            
            # 3. Establecer FOREIGN KEYs
            print("Estableciendo claves foráneas...")
            connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (date_id) REFERENCES {DIM_DATE_NAME} (date_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (symbol_id) REFERENCES {DIM_SYMBOL_NAME} (symbol_id);"))

        print("🎉 Normalización y Carga completadas para Precios de Acciones con Esquema Estrella.")

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
        print("Pipeline finalizado sin datos para procesar.")
        return

    # 3. Transformación y Normalización
    df_fact_table, df_dim_date, df_dim_symbol = transform_data(df_raw)
    
    if df_fact_table.empty:
        print("Pipeline finalizado, no hay datos válidos después de la limpieza.")
        return

    # 4. Carga a S3
    load_to_s3(df_fact_table)

    # 5. Carga a RDS y Establecimiento de Claves
    load_to_rds(engine, df_fact_table, df_dim_date, df_dim_symbol)


if __name__ == "__main__":
    run_etl_pipeline()