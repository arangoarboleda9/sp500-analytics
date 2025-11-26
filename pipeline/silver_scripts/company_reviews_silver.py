import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import numpy as np 
import boto3
from io import BytesIO 
from config import Config
import config
# Cargar variables de entorno (para AWS Secrets y RDS)
load_dotenv() 

# -----------------------------------------------------
# ⚠️ CONFIGURACIÓN DE CREDENCIALES AWS y DESTINO S3 ⚠️
# -----------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = config.AWS_DEFAULT_REGION         
SILVER_BUCKET_NAME = config.S3_BUCKET         
S3_KEY_PATH = "silver/company_reviews/fact_company_reviews.parquet" 

# -----------------------------------------------------
# CONFIGURACIÓN DE RDS Y TABLAS (Usando la clase Config)
# -----------------------------------------------------
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# --- CORRECCIÓN DE ERROR DE ENTORNO (DB_PORT 'None') ---
if DB_PORT is None or not str(DB_PORT).isdigit(): 
    print("❌ ERROR: La variable DB_PORT no se cargó correctamente o no es un número.")
    print("Asegúrate de que tu archivo .env existe y contiene DB_PORT=\"5432\" (o el puerto correcto).")
    exit()
# -----------------------------------------------------


# Nombres de las tablas
RAW_REVIEWS_TABLE = "company_reviews" 
FACT_REVIEWS_NAME = "fact_company_reviews" 
FACT_COMPANY_INFO_NAME = "fact_company_info" 

# Dimensiones
DIM_INDUSTRY_NAME = "dim_review_industry"
DIM_HQ_NAME = "dim_headquarters"
DIM_CEO_APPR_NAME = "dim_ceo_approval"


# -----------------------------------------------------
# Conectar a RDS PostgreSQL
# -----------------------------------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Paso A: Extracción (E) - Leer datos y FILTRAR por compañía existente
# -----------------------------------------------------
print(f"Paso A: Extrayendo datos y filtrando mediante limpieza agresiva de nombres...")

# 💡 CORRECCIÓN DE LÓGICA: LIMPIEZA SQL MUY AGRESIVA PARA AUMENTAR COINCIDENCIAS
# Se eliminan términos comunes que causan inconsistencias en el JOIN.
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
                                                        r.name, 
                                                        ' and ', ''
                                                    ), 
                                                    ' & ', 'and' -- P&G -> P and G
                                                ), 
                                                ' services', '' -- Amazon Web Services -> Amazon Web
                                            ), 
                                            ' stores', '' -- Walmart Stores -> Walmart
                                        ), 
                                        ' solutions', ''
                                    ), 
                                    ' global', ''
                                ),
                                ' technologies', ''
                            ),
                            ',', ''
                        ), 
                        '.', ''
                    ),
                    ' inc', ''
                ), 
                ' corp', ''
            ),
            ' co', ''
        )
    )
"""

# Limpieza básica para la tabla de Info
CLEAN_SHORTNAME_SQL = """
    LOWER(
        REPLACE(
            REPLACE(
                c.shortname, 
                ',', ''
            ), 
            '.', ''
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
        df_fact = pd.read_sql(query, connection)
except Exception as e:
    print(f"❌ Error al extraer/filtrar datos de RDS: {e}")
    exit()

print(f"Datos extraídos y filtrados. Filas después de la limpieza agresiva: {len(df_fact)}")

# -----------------------------------------------------
# Paso B: Transformación (T) - Limpieza, Ajustes y NORMALIZACIÓN
# -----------------------------------------------------
print("Paso B: Aplicando transformaciones y normalización (Esquema Estrella)...")

# 1. Limpieza de nombres de columnas a snake_case
df_fact.columns = [col.lower().replace(' ', '_').replace('.', '').replace('-', '_') for col in df_fact.columns]

column_names = df_fact.columns.tolist()

# 2. Definición de Tipos y Limpieza
numeric_cols = ['rating']
for col in numeric_cols:
    if col in df_fact.columns:
        df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce')

# --- CORRECCIÓN CRÍTICA DE TIPO DE DATO (company_id) ---
df_fact['company_id'] = pd.to_numeric(df_fact['company_id'], errors='coerce')
print("✅ company_id convertido a tipo numérico.")
# -------------------------------------------------------
        
# LIMPIEZA DE TEXTO Y ESTANDARIZACIÓN
for col in column_names:
    if col not in df_fact.columns or col in numeric_cols:
        continue
        
    df_fact[col] = df_fact[col].astype(str).str.strip()
    
    if col in ['name', 'industry', 'ceo_approval', 'headquarters', 'revenue']:
        df_fact[col] = df_fact[col].str.title()
        
    if col in ['reviews', 'ceo_count', 'interview_difficulty', 'interview_duration', 'interview_count', 'employees', 'salary', 'locations', 'roles']:
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

# Selección final de columnas para la TABLA DE HECHOS
fact_columns = [
    'review_id',         
    'company_id',        
    'rating',            
    'reviews',           
    'ceo_count',         
    'interview_count',   
    'salary',            
    'description',
    'happiness',
    'ratings',
    'locations',
    'roles',
    'interview_experience',
    'interview_difficulty',
    'interview_duration',
    'employees',
    'revenue',
    'website',
    'review_industry_id',   
    'headquarters_id',      
    'ceo_approval_id'       
]

df_fact_table = df_fact[[col for col in fact_columns if col in df_fact.columns]]


print(f"DataFrame de Hechos listo. Filas: {len(df_fact_table)}")
print("Tipos de datos finales de la Tabla de Hechos:")
print(df_fact_table.dtypes)

# -----------------------------------------------------
# Paso C: Carga a S3 (Data Lake - Carga Principal)
# -----------------------------------------------------
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
    s3_client.put_object(
        Bucket=SILVER_BUCKET_NAME,
        Key=S3_KEY_PATH,
        Body=parquet_buffer.getvalue()
    )

    print(f"✔ Carga en S3 exitosa: s3://{SILVER_BUCKET_NAME}/{S3_KEY_PATH}")

except Exception as e:
    print(f"❌ Error al cargar a S3: {e}")

# -----------------------------------------------------
# Paso D: Carga a RDS (Data Warehouse/Análisis - Carga Secundaria)
# -----------------------------------------------------
print(f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella")

tables_to_load = [
    (df_dim_industry, DIM_INDUSTRY_NAME),
    (df_dim_hq, DIM_HQ_NAME),
    (df_dim_ceo, DIM_CEO_APPR_NAME),
    (df_fact_table, FACT_REVIEWS_NAME), 
]

try:
    with engine.begin() as connection:
        
        # 💡 CORRECCIÓN CRÍTICA PARA DEPENDENT OBJECTS (DROP CASCADE)
        print("Limpiando tablas existentes (DROP CASCADE)...")
        connection.execute(text(f"DROP TABLE IF EXISTS {FACT_REVIEWS_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_INDUSTRY_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_HQ_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_CEO_APPR_NAME} CASCADE;"))
        print("Limpieza completada.")


        # 1. Cargar las tablas de Dimensión
        for df, table_name in tables_to_load[:-1]:
             df.to_sql(table_name, connection, if_exists="replace", index=False)
             print(f"✔ Dimensión '{table_name}' cargada.")

        # 2. Cargar la Tabla de Hechos
        df_fact_table.to_sql(FACT_REVIEWS_NAME, connection, if_exists="replace", index=False)
        print(f"✔ Tabla de Hechos '{FACT_REVIEWS_NAME}' cargada.")

        # Establecer PRIMARY KEYs y FOREIGN KEYs
        print("Estableciendo claves...")
        connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD PRIMARY KEY (review_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_INDUSTRY_NAME} ADD PRIMARY KEY (review_industry_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_HQ_NAME} ADD PRIMARY KEY (headquarters_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_CEO_APPR_NAME} ADD PRIMARY KEY (ceo_approval_id);"))
        
        # FKs
        connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (company_id) REFERENCES {FACT_COMPANY_INFO_NAME} (company_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (review_industry_id) REFERENCES {DIM_INDUSTRY_NAME} (review_industry_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (headquarters_id) REFERENCES {DIM_HQ_NAME} (headquarters_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_REVIEWS_NAME} ADD FOREIGN KEY (ceo_approval_id) REFERENCES {DIM_CEO_APPR_NAME} (ceo_approval_id);"))


    print("🎉 Normalización y Carga completadas para Reviews con Esquema Estrella.")

except Exception as e:
    print(f"❌ Error al cargar a RDS: {e}")