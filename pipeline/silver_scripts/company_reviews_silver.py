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
S3_KEY_PATH = "silver/company_reviews/fact_company_reviews.parquet" 

# Credenciales de RDS (Inyectadas directamente desde el módulo config)
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# Nombres de las tablas
RAW_REVIEWS_TABLE = "company_reviews" 
FACT_REVIEWS_NAME = "fact_company_reviews" 
FACT_COMPANY_INFO_NAME = "fact_company_info" 

# Dimensiones
DIM_INDUSTRY_NAME = "dim_review_industry"
DIM_HQ_NAME = "dim_headquarters"
DIM_CEO_APPR_NAME = "dim_ceo_approval"


def get_db_engine():
    """Configura y retorna el objeto Engine para la conexión a RDS PostgreSQL."""
    try:
        # Verifica la configuración de la base de datos
        if DB_HOST == config.NOT_SET or DB_PORT == config.NOT_SET or not str(DB_PORT).isdigit():
            raise EnvironmentError("Las variables de entorno de RDS no están configuradas correctamente (DB_HOST, DB_PORT, etc.).")
            
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        print("✔ Conexión a la base de datos RDS establecida.")
        return engine
    except Exception as e:
        print(f"❌ Error al conectar a RDS: {e}")
        return None


def extract_data(engine):
    """
    Extrae datos de la capa Raw aplicando un JOIN mediante limpieza agresiva 
    de nombres directamente en la consulta SQL.
    """
    print(f"\nPaso A: Extrayendo datos y filtrando mediante limpieza agresiva de nombres...")

    # --- LÓGICA DE LIMPIEZA SQL PRESERVADA ---
    CLEAN_REVIEW_NAME_SQL = """
        LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                REPLACE(
                                                    REPLACE(
                                                        REPLACE(
                                                            r.name, ' and ', ''
                                                        ), ' & ', 'and'
                                                    ), ' services', ''
                                                ), ' stores', ''
                                            ), ' solutions', ''
                                        ), ' global', ''
                                    ), ' technologies', ''
                                ), ',', ''
                            ), '.', ''
                        ), ' inc', ''
                    ), ' corp', ''
                ), ' co', ''
            )
        )
    """
    CLEAN_SHORTNAME_SQL = """
        LOWER(
            REPLACE(
                REPLACE(
                    c.shortname, ',', ''
                ), '.', ''
            )
        )
    """

    query = f"""
        SELECT 
            r.*, 
            c.company_id,
            c.symbol AS company_symbol
        FROM {RAW_REVIEWS_TABLE} r
        INNER JOIN {FACT_COMPANY_INFO_NAME} c 
            ON {CLEAN_REVIEW_NAME_SQL} = {CLEAN_SHORTNAME_SQL}; 
    """

    try:
        with engine.connect() as connection:
            df_fact = pd.read_sql(text(query), connection)
        print(f"✔ Datos extraídos y filtrados. Filas después del JOIN: {len(df_fact)}")
        return df_fact
    except Exception as e:
        print(f"❌ Error al extraer/filtrar datos de RDS: {e}")
        return pd.DataFrame()


def transform_data(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Aplica limpieza, ajustes y la NORMALIZACIÓN del Esquema Estrella.
    Retorna la Tabla de Hechos y las tres Tablas de Dimensión.
    """
    if df_raw.empty:
        print("DataFrame vacío, omitiendo transformación.")
        # Retorna 4 DataFrames vacíos para evitar errores de desempaquetado
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame() 

    print("\nPaso B: Aplicando transformaciones y normalización (Esquema Estrella)...")
    df_fact = df_raw.copy()

    # 1. Limpieza de nombres de columnas a snake_case
    df_fact.columns = [col.lower().replace(' ', '_').replace('.', '').replace('-', '_') for col in df_fact.columns]
    column_names = df_fact.columns.tolist()

    # 2. Definición de Tipos y Limpieza
    df_fact['rating'] = pd.to_numeric(df_fact['rating'], errors='coerce')
    df_fact['company_id'] = pd.to_numeric(df_fact['company_id'], errors='coerce')

    # LIMPIEZA DE TEXTO Y ESTANDARIZACIÓN
    for col in column_names:
        if col not in df_fact.columns or col == 'rating' or col == 'company_id':
            continue
            
        df_fact[col] = df_fact[col].astype(str).str.strip()
        
        if col in ['name', 'industry', 'ceo_approval', 'headquarters', 'revenue']:
            df_fact[col] = df_fact[col].str.title()
            
        if col in ['reviews', 'ceo_count', 'interview_difficulty', 'interview_duration', 'interview_count', 'employees', 'salary', 'locations', 'roles']:
            # Reemplazar 'nan' o 'none' como texto por NaN de Pandas
            df_fact.loc[df_fact[col].str.lower() == 'nan', col] = np.nan
            df_fact.loc[df_fact[col].str.lower() == 'none', col] = np.nan

    # 3. Eliminar filas con información crítica faltante y asegurar tipo INT para la clave.
    df_fact.dropna(subset=['company_id', 'rating'], inplace=True)
    df_fact['company_id'] = df_fact['company_id'].astype(np.int64) 

    # --- NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

    # 1. DIM_REVIEW_INDUSTRY
    df_dim_industry = df_fact[['industry']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_industry['review_industry_id'] = df_dim_industry.index + 1
    df_dim_industry = df_dim_industry[['review_industry_id', 'industry']]
    df_fact = pd.merge(df_fact, df_dim_industry, on='industry', how='left')

    # 2. DIM_HEADQUARTERS
    df_dim_hq = df_fact[['headquarters']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_hq['headquarters_id'] = df_dim_hq.index + 1
    df_dim_hq = df_dim_hq[['headquarters_id', 'headquarters']]
    df_fact = pd.merge(df_fact, df_dim_hq, on='headquarters', how='left')

    # 3. DIM_CEO_APPROVAL
    df_dim_ceo = df_fact[['ceo_approval']].drop_duplicates().dropna().reset_index(drop=True)
    df_dim_ceo['ceo_approval_id'] = df_dim_ceo.index + 1
    df_dim_ceo = df_dim_ceo[['ceo_approval_id', 'ceo_approval']]
    df_fact = pd.merge(df_fact, df_dim_ceo, on='ceo_approval', how='left')

    # 4. CREACIÓN DE LA TABLA DE HECHOS (FACT_COMPANY_REVIEWS)
    df_fact = df_fact.sort_values(by='rating', ascending=False).reset_index(drop=True)
    df_fact['review_id'] = df_fact.index + 1

    fact_columns = [
        'review_id', 'company_id', 'rating', 'reviews', 'ceo_count', 'interview_count', 'salary', 
        'description', 'happiness', 'ratings', 'locations', 'roles', 'interview_experience',
        'interview_difficulty', 'interview_duration', 'employees', 'revenue', 'website',
        'review_industry_id', 'headquarters_id', 'ceo_approval_id'       
    ]

    df_fact_table = df_fact[[col for col in fact_columns if col in df_fact.columns]]
    print(f"✔ Normalización completa. DataFrame de Hechos listo. Filas: {len(df_fact_table)}")
    
    return df_fact_table, df_dim_industry, df_dim_hq, df_dim_ceo


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
            Body=parquet_buffer.getvalue() # Usar getvalue() para leer el buffer
        )

        print(f"✔ Carga en S3 exitosa: s3://{SILVER_BUCKET_NAME}/{S3_KEY_PATH}")

    except Exception as e:
        print(f"❌ Error al cargar a S3: {e}")


def load_to_rds(engine, df_fact_table: pd.DataFrame, df_dim_industry: pd.DataFrame, df_dim_hq: pd.DataFrame, df_dim_ceo: pd.DataFrame):
    """Carga Tablas de Dimensión y Hechos a RDS y establece las claves."""
    if df_fact_table.empty or engine is None:
        print("DataFrame o motor de DB no válidos, omitiendo carga a RDS.")
        return

    print(f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella")

    tables_to_load = [
        (df_dim_industry, DIM_INDUSTRY_NAME),
        (df_dim_hq, DIM_HQ_NAME),
        (df_dim_ceo, DIM_CEO_APPR_NAME),
        (df_fact_table, FACT_REVIEWS_NAME), 
    ]

    try:
        with engine.begin() as connection:
            
            # Limpiar tablas existentes para asegurar un REPLACE limpio con dependencias (CASCADE)
            print("Limpiando tablas existentes (DROP CASCADE)...")
            connection.execute(text(f"DROP TABLE IF EXISTS {FACT_REVIEWS_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_INDUSTRY_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_HQ_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_CEO_APPR_NAME} CASCADE;"))
            print("Limpieza completada.")

            # 1. Cargar las tablas de Dimensión y la de Hechos
            for df, table_name in tables_to_load:
                 df.to_sql(table_name, connection, if_exists="replace", index=False)
                 print(f"✔ Tabla '{table_name}' cargada.")

            # 2. Establecer PRIMARY KEYs
            print("Estableciendo claves primarias...")
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD PRIMARY KEY (review_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_INDUSTRY_NAME} ADD PRIMARY KEY (review_industry_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_HQ_NAME} ADD PRIMARY KEY (headquarters_id);"))
            connection.execute(text(f"ALTER TABLE {DIM_CEO_APPR_NAME} ADD PRIMARY KEY (ceo_approval_id);"))
            
            # 3. Establecer FOREIGN KEYs
            print("Estableciendo claves foráneas...")
            # FK hacia FACT_COMPANY_INFO
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (company_id) REFERENCES {FACT_COMPANY_INFO_NAME} (company_id);"))
            # FKs hacia Dimensiones de Review
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (review_industry_id) REFERENCES {DIM_INDUSTRY_NAME} (review_industry_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (headquarters_id) REFERENCES {DIM_HQ_NAME} (headquarters_id);"))
            connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (ceo_approval_id) REFERENCES {DIM_CEO_APPR_NAME} (ceo_approval_id);"))

        print("🎉 Normalización y Carga completadas para Reviews con Esquema Estrella.")

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
    df_fact_table, df_dim_industry, df_dim_hq, df_dim_ceo = transform_data(df_raw)
    
    if df_fact_table.empty:
        print("Pipeline finalizado, no hay datos válidos después del JOIN y la limpieza.")
        return

    # 4. Carga a S3
    load_to_s3(df_fact_table)

    # 5. Carga a RDS y Establecimiento de Claves
    load_to_rds(engine, df_fact_table, df_dim_industry, df_dim_hq, df_dim_ceo)


if __name__ == "__main__":
    run_etl_pipeline()