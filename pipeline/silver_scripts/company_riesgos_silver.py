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
S3_KEY_PATH = "silver/company_risk_analysis/fact_company_risk.parquet" 

# Credenciales de RDS (Inyectadas directamente desde el módulo config)
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# Nombres de las tablas
RAW_TABLE_NAME = "company_risk_analysis" 
FACT_RISK_NAME = "fact_company_risk" 
DIM_SECTOR_NAME = "dim_sector_risk" # Renombrado para evitar colisión con company_info
DIM_INDUSTRY_NAME = "dim_industry_risk" # Renombrado para evitar colisión con company_info
DIM_ESG_LEVEL_NAME = "dim_esg_level"
DIM_CONTROV_NAME = "dim_controversy_level"


def get_db_engine():
    """Configura y retorna el objeto Engine para la conexión a RDS PostgreSQL."""
    try:
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
            df_fact = pd.read_sql(query, connection)
        print(f"✔ Datos extraídos. Filas: {len(df_fact)}")
        return df_fact
    except Exception as e:
        print(f"❌ Error al extraer datos de RDS: {e}")
        return pd.DataFrame()


def clean_col_name(col):
    """Función auxiliar para renombrar columnas específicas de riesgo."""
    col = col.lower().replace(' ', '_').replace('.', '').replace('-', '_')
    col = col.replace('total_esg_risk_score', 'esg_risk_score')
    col = col.replace('environment_risk_score', 'env_risk_score')
    col = col.replace('governance_risk_score', 'gov_risk_score')
    col = col.replace('social_risk_score', 'soc_risk_score')
    col = col.replace('esg_risk_percentile', 'esg_percentile')
    col = col.replace('esg_risk_level', 'esg_level')
    return col


def transform_data(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aplica limpieza, ajustes y la NORMALIZACIÓN del Esquema Estrella."""
    
    if df_raw.empty:
        print("DataFrame vacío, omitiendo transformación.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame() 

    print("\nPaso B: Aplicando transformaciones y normalización (Esquema Estrella)...")
    df_fact = df_raw.copy()

    # 1. Limpieza de nombres de columnas
    df_fact.columns = [clean_col_name(col) for col in df_fact.columns]
    column_names = df_fact.columns.tolist()

    # 2. Definición de Tipos y Limpieza
    numeric_cols = [
        'full_time_employees', 'esg_risk_score', 'env_risk_score', 'gov_risk_score', 
        'soc_risk_score', 'controversy_score', 'esg_percentile'
    ]

    for col in numeric_cols:
        if col in df_fact.columns:
            df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce')
            
    # LIMPIEZA DE TEXTO Y ESTANDARIZACIÓN
    for col in column_names:
        if col not in df_fact.columns or col in numeric_cols:
            continue
            
        df_fact[col] = df_fact[col].astype(str).str.strip()
        
        # Capitalización (para las futuras dimensiones)
        if col in ['name', 'sector', 'industry', 'esg_level', 'controversy_level']:
            df_fact[col] = df_fact[col].str.title()
            
        # Símbolo en mayúsculas
        if col == 'symbol':
            df_fact[col] = df_fact[col].str.upper()

    # 3. Eliminar filas con información crítica faltante
    df_fact.dropna(subset=['symbol', 'esg_risk_score', 'sector', 'industry'], inplace=True)
    df_fact = df_fact.reset_index(drop=True)

    # --- NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

    # 1. DIM_SECTOR
    df_dim_sector = df_fact[['sector']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_sector['sector_id'] = df_dim_sector.index + 1
    df_dim_sector = df_dim_sector[['sector_id', 'sector']]
    df_fact = pd.merge(df_fact, df_dim_sector, on='sector', how='left')

    # 2. DIM_INDUSTRY
    df_dim_industry = df_fact[['industry']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_industry['industry_id'] = df_dim_industry.index + 1
    df_dim_industry = df_dim_industry[['industry_id', 'industry']]
    df_fact = pd.merge(df_fact, df_dim_industry, on='industry', how='left')

    # 3. DIM_ESG_LEVEL
    df_dim_esg = df_fact[['esg_level']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_esg['esg_level_id'] = df_dim_esg.index + 1
    df_dim_esg = df_dim_esg[['esg_level_id', 'esg_level']]
    df_fact = pd.merge(df_fact, df_dim_esg, on='esg_level', how='left')

    # 4. DIM_CONTROVERSY_LEVEL
    df_dim_controv = df_fact[['controversy_level']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_controv['controversy_level_id'] = df_dim_controv.index + 1
    df_dim_controv = df_dim_controv[['controversy_level_id', 'controversy_level']]
    df_fact = pd.merge(df_fact, df_dim_controv, on='controversy_level', how='left')

    # 5. CREACIÓN DE LA TABLA DE HECHOS (FACT_COMPANY_RISK)
    df_fact = df_fact.sort_values(by='symbol').reset_index(drop=True)
    df_fact['risk_id'] = df_fact.index + 1

    fact_columns = [
        'risk_id', 'symbol', 'name', 'full_time_employees', 'esg_risk_score', 'env_risk_score', 
        'gov_risk_score', 'soc_risk_score', 'controversy_score', 'esg_percentile', 
        'sector_id', 'industry_id', 'esg_level_id', 'controversy_level_id'     
    ]

    df_fact_table = df_fact[[col for col in fact_columns if col in df_fact.columns]]

    print(f"✔ Normalización completa. DataFrame de Hechos listo. Filas: {len(df_fact_table)}")

    return df_fact_table, df_dim_sector, df_dim_industry, df_dim_esg, df_dim_controv


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


def load_to_rds(engine, df_fact_table: pd.DataFrame, df_dim_sector: pd.DataFrame, df_dim_industry: pd.DataFrame, df_dim_esg: pd.DataFrame, df_dim_controv: pd.DataFrame):
    """Carga Tablas de Dimensión y Hechos a RDS y establece las claves."""
    if df_fact_table.empty or engine is None:
        print("DataFrame o motor de DB no válidos, omitiendo carga a RDS.")
        return

    print(f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella")

    tables_to_load = [
        (df_dim_sector, DIM_SECTOR_NAME),
        (df_dim_industry, DIM_INDUSTRY_NAME),
        (df_dim_esg, DIM_ESG_LEVEL_NAME),
        (df_dim_controv, DIM_CONTROV_NAME),
        (df_fact_table, FACT_RISK_NAME), 
    ]

    try:
        with engine.begin() as connection:
            
            # Limpiar tablas existentes (DROP CASCADE)
            print("Limpiando tablas existentes (DROP CASCADE)...")
            connection.execute(text(f"DROP TABLE IF EXISTS {FACT_RISK_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_SECTOR_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_INDUSTRY_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_ESG_LEVEL_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_CONTROV_NAME} CASCADE;"))
            print("Limpieza completada.")

            # 1. Cargar las tablas de Dimensión y la de Hechos
            for df, table_name in tables_to_load:
                 df.to_sql(table_name, connection, if_exists="replace", index=False)
                 print(f"✔ Tabla '{table_name}' cargada.")

            # 2. Establecer PRIMARY KEYs
            print("Estableciendo claves primarias...")
            connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD PRIMARY KEY (risk_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_SECTOR_NAME} ADD PRIMARY KEY (sector_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_INDUSTRY_NAME} ADD PRIMARY KEY (industry_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_ESG_LEVEL_NAME} ADD PRIMARY KEY (esg_level_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_CONTROV_NAME} ADD PRIMARY KEY (controversy_level_id);"))
            
            # 3. Establecer FOREIGN KEYs
            print("Estableciendo claves foráneas...")
            connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (sector_id) REFERENCES {DIM_SECTOR_NAME} (sector_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (industry_id) REFERENCES {DIM_INDUSTRY_NAME} (industry_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (esg_level_id) REFERENCES {DIM_ESG_LEVEL_NAME} (esg_level_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (controversy_level_id) REFERENCES {DIM_CONTROV_NAME} (controversy_level_id);"))

        print("🎉 Normalización y Carga completadas para Análisis de Riesgo con Esquema Estrella.")

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
    df_fact_table, df_dim_sector, df_dim_industry, df_dim_esg, df_dim_controv = transform_data(df_raw)
    
    if df_fact_table.empty:
        print("Pipeline finalizado, no hay datos válidos después de la limpieza.")
        return

    # 4. Carga a S3
    load_to_s3(df_fact_table)

    # 5. Carga a RDS y Establecimiento de Claves
    load_to_rds(engine, df_fact_table, df_dim_sector, df_dim_industry, df_dim_esg, df_dim_controv)


if __name__ == "__main__":
    run_etl_pipeline()