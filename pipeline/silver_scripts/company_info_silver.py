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

# Cargar variables de entorno (para inicializar el módulo config)
load_dotenv() 

# -----------------------------------------------------
# ⚙️ CONFIGURACIÓN GLOBAL (Sourced desde el módulo config)
# -----------------------------------------------------

# Credenciales AWS/S3 (Inyectadas desde el módulo config)
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_DEFAULT_REGION
SILVER_BUCKET_NAME = config.S3_BUCKET 
S3_KEY_PATH = "silver/company_reviews/fact_company_reviews.parquet" 

# Credenciales de RDS (Inyectadas desde el módulo config)
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# Nombres de las tablas
RAW_REVIEWS_NAME = "company_reviews"
FACT_COMPANY_INFO_NAME = "fact_company_info" # Necesaria para el JOIN
FACT_REVIEWS_NAME = "fact_company_reviews"
DIM_INDUSTRY_NAME = "dim_review_industry"
DIM_HQ_NAME = "dim_headquarters"
DIM_CEO_APPR_NAME = "dim_ceo_approval"


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
    """Extrae datos de la capa Raw y la tabla maestra de Hechos (para JOIN)."""
    print(f"\nPaso A: Extrayendo datos de la tabla Raw: {RAW_REVIEWS_NAME}")
    
    query_reviews = f"SELECT * FROM {RAW_REVIEWS_NAME};"
    query_fact = f"SELECT company_id, symbol FROM {FACT_COMPANY_INFO_NAME};"

    try:
        with engine.connect() as connection:
            df_reviews_raw = pd.read_sql(query_reviews, connection)
            df_fact_master = pd.read_sql(query_fact, connection)
        
        print(f"✔ Datos de Reviews extraídos. Filas: {len(df_reviews_raw)}")
        print(f"✔ Datos de la Tabla Maestra ({FACT_COMPANY_INFO_NAME}) extraídos. Filas: {len(df_fact_master)}")
        return df_reviews_raw, df_fact_master
    except Exception as e:
        print(f"❌ Error al extraer datos de RDS: {e}")
        return pd.DataFrame(), pd.DataFrame()


def transform_data(df_reviews_raw: pd.DataFrame, df_fact_master: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aplica limpieza, estandarización y normalización (JOIN y creación de dimensiones)."""
    
    if df_reviews_raw.empty or df_fact_master.empty:
        print("DataFrames de entrada vacíos, omitiendo transformación.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    df_reviews = df_reviews_raw.copy()
    print("\nPaso B: Aplicando transformaciones y normalización...")

    # 1. Limpieza de nombres de columnas
    df_reviews.columns = [col.lower().replace(' ', '_').replace('.', '').replace('-', '_') for col in df_reviews.columns]

    # 2. Renombrar ID único
    df_reviews = df_reviews.rename(columns={'id': 'review_id'}, errors='ignore')

    # 3. Conversión de Tipos y Limpieza de Texto
    numeric_cols = ['overall_rating', 'work_life_balance', 'culture_values', 'diversity_inclusion', 'career_opportunities', 'compensation_benefits', 'senior_management']
    
    for col in numeric_cols:
        if col in df_reviews.columns:
            # Reemplazar valores no numéricos (ej. '-1') por NaN antes de la conversión
            df_reviews[col] = df_reviews[col].replace('[-]', np.nan, regex=True)
            df_reviews[col] = pd.to_numeric(df_reviews[col], errors='coerce')
    
    # Limpieza de columnas de texto
    text_cols = ['ticker', 'industry', 'headquarters', 'ceo_approval']
    for col in text_cols:
        if col in df_reviews.columns:
            df_reviews[col] = df_reviews[col].astype(str).str.strip().str.title()

    # 4. JOIN con la tabla maestra para obtener company_id
    # Alineamos 'ticker' con 'symbol' (clave de unión)
    df_reviews = df_reviews.rename(columns={'ticker': 'symbol'})
    
    df_merged = pd.merge(
        df_reviews, 
        df_fact_master, 
        on='symbol', 
        how='inner' # SOLO mantenemos reviews que tienen un match en fact_company_info
    )
    print(f"✔ JOIN completado. Filas después de la unión: {len(df_merged)}")


    # --- PASO DE NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

    # 1. DIM_REVIEW_INDUSTRY
    df_dim_industry = df_merged[['industry']].drop_duplicates().reset_index(drop=True)
    df_dim_industry['review_industry_id'] = df_dim_industry.index + 1
    df_dim_industry = df_dim_industry[['review_industry_id', 'industry']]
    df_merged = pd.merge(df_merged, df_dim_industry, on='industry', how='left')

    # 2. DIM_HEADQUARTERS
    df_dim_hq = df_merged[['headquarters']].drop_duplicates().reset_index(drop=True)
    df_dim_hq['headquarters_id'] = df_dim_hq.index + 1
    df_dim_hq = df_dim_hq[['headquarters_id', 'headquarters']]
    df_merged = pd.merge(df_merged, df_dim_hq, on='headquarters', how='left')

    # 3. DIM_CEO_APPROVAL
    df_dim_ceo_appr = df_merged[['ceo_approval']].drop_duplicates().reset_index(drop=True)
    df_dim_ceo_appr['ceo_approval_id'] = df_dim_ceo_appr.index + 1
    df_dim_ceo_appr = df_dim_ceo_appr[['ceo_approval_id', 'ceo_approval']]
    df_merged = pd.merge(df_merged, df_dim_ceo_appr, on='ceo_approval', how='left')


    # 4. CREACIÓN DE LA TABLA DE HECHOS (FACT_COMPANY_REVIEWS)
    # Generamos un nuevo ID de reseña si el original no existía o era defectuoso
    if 'review_id' not in df_merged.columns or df_merged['review_id'].isnull().all():
        df_merged['review_id'] = df_merged.reset_index().index + 1
    
    # Selección final de columnas para la TABLA DE HECHOS
    fact_columns = [
        'review_id', # Primary Key 
        'company_id', # Foreign Key al fact_company_info
        'overall_rating', 
        'work_life_balance', 
        'culture_values', 
        'diversity_inclusion', 
        'career_opportunities', 
        'compensation_benefits', 
        'senior_management',
        'review_industry_id',   # Foreign Key
        'headquarters_id',      # Foreign Key
        'ceo_approval_id'       # Foreign Key
    ]

    df_fact_table = df_merged[[col for col in fact_columns if col in df_merged.columns]]

    print(f"✔ Normalización completa. DataFrame de Hechos listo. Filas: {len(df_fact_table)}")
    
    return df_fact_table, df_dim_industry, df_dim_hq, df_dim_ceo_appr


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
        parquet_buffer.seek(0)

        s3_client.put_object(
            Bucket=SILVER_BUCKET_NAME,
            Key=S3_KEY_PATH,
            Body=parquet_buffer.read()
        )

        print(f"✔ Carga en S3 exitosa: s3://{SILVER_BUCKET_NAME}/{S3_KEY_PATH}")

    except Exception as e:
        print(f"❌ Error al cargar a S3: {e}")


def load_to_rds(engine, df_fact_table: pd.DataFrame, df_dim_industry: pd.DataFrame, df_dim_hq: pd.DataFrame, df_dim_ceo_appr: pd.DataFrame):
    """Carga Tablas de Dimensión y Hechos a RDS y establece las claves."""
    if df_fact_table.empty or engine is None:
        print("DataFrame o motor de DB no válidos, omitiendo carga a RDS.")
        return

    print(f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella")

    tables_to_load = [
        (df_dim_industry, DIM_INDUSTRY_NAME),
        (df_dim_hq, DIM_HQ_NAME),
        (df_dim_ceo_appr, DIM_CEO_APPR_NAME),
        (df_fact_table, FACT_REVIEWS_NAME), # Hechos al final
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
            connection.execute(text(f"ALTER TABLE {DIM_INDUSTRY_NAME} ADD PRIMARY KEY (review_industry_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_HQ_NAME} ADD PRIMARY KEY (headquarters_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_CEO_APPR_NAME} ADD PRIMARY KEY (ceo_approval_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD PRIMARY KEY (review_id);"))

            # FOREIGN KEYs (FKs)
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (company_id) REFERENCES {FACT_COMPANY_INFO_NAME} (company_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (review_industry_id) REFERENCES {DIM_INDUSTRY_NAME} (review_industry_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (headquarters_id) REFERENCES {DIM_HQ_NAME} (headquarters_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (ceo_approval_id) REFERENCES {DIM_CEO_APPR_NAME} (ceo_approval_id);"))

        print("🎉 Carga a RDS completadas con Esquema Estrella para Reviews.")

    except Exception as e:
        print(f"❌ Error al cargar a RDS o al establecer claves: {e}")


def run_etl_pipeline():
    """Orquesta la ejecución completa del pipeline ETL."""
    
    # 1. Obtener motor de DB
    engine = get_db_engine()
    if engine is None:
        return

    # 2. Extracción (Reviews Raw y Fact Master)
    df_reviews_raw, df_fact_master = extract_data(engine)
    if df_reviews_raw.empty or df_fact_master.empty:
        print("Pipeline finalizado por falta de datos necesarios.")
        return

    # 3. Transformación y Normalización
    df_fact_table, df_dim_industry, df_dim_hq, df_dim_ceo_appr = transform_data(df_reviews_raw, df_fact_master)
    
    if df_fact_table.empty:
        print("Pipeline finalizado, no hay datos válidos después del JOIN y la limpieza.")
        return

    # 4. Carga a S3
    load_to_s3(df_fact_table)

    # 5. Carga a RDS y Establecimiento de Claves
    load_to_rds(engine, df_fact_table, df_dim_industry, df_dim_hq, df_dim_ceo_appr)


if __name__ == "__main__":
    run_etl_pipeline()