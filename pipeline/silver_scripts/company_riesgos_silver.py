import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import numpy as np 
import boto3
from io import BytesIO 

# -----------------------------------------------------
# ⚠️ CONFIGURACIÓN DE CREDENCIALES AWS y DESTINO S3 ⚠️
# -----------------------------------------------------
# Credenciales de Acceso AWS (INYECCIÓN MANUAL)
AWS_ACCESS_KEY_ID = "" 
AWS_SECRET_ACCESS_KEY = ""
AWS_REGION = "us-east-1" 
SILVER_BUCKET_NAME = "henry-sp500-dataset"  
# RUTA S3 CORRECTA PARA ANÁLISIS DE RIESGO
S3_KEY_PATH = "silver/company_risk_analysis/fact_company_risk.parquet" 

# -----------------------------------------------------
# CONFIGURACIÓN DE RDS Y TABLAS
# -----------------------------------------------------
load_dotenv() 

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST") 
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# --- VERIFICACIÓN DE ERROR DE ENTORNO (DB_PORT 'None') ---
if DB_PORT is None or not DB_PORT.isdigit():
    print("❌ ERROR: La variable DB_PORT no se cargó correctamente o no es un número.")
    print("Asegúrate de que tu archivo .env existe y contiene DB_PORT=\"5432\" (o el puerto correcto).")
    exit()
# ----------------------------------------------------------

# Nombres de las tablas
RAW_TABLE_NAME = "company_risk_analysis" 
FACT_RISK_NAME = "fact_company_risk" # Tabla de Hechos
# Nombres de las dimensiones
DIM_SECTOR_NAME = "dim_sector"
DIM_INDUSTRY_NAME = "dim_industry"
DIM_ESG_LEVEL_NAME = "dim_esg_level"
DIM_CONTROV_NAME = "dim_controversy_level"


# -----------------------------------------------------
# Conectar a RDS PostgreSQL
# -----------------------------------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Paso A: Extracción (E) - Leer datos desde la capa Raw
# -----------------------------------------------------
print(f"Paso A: Extrayendo datos de la tabla Raw: {RAW_TABLE_NAME}")

query = f"SELECT * FROM {RAW_TABLE_NAME};"

try:
    with engine.connect() as connection:
        df_fact = pd.read_sql(query, connection)
except Exception as e:
    print(f"❌ Error al extraer datos de RDS: {e}")
    exit()

print(f"Datos extraídos. Filas: {len(df_fact)}")

# -----------------------------------------------------
# Paso B: Transformación (T) - Limpieza y Esquema Estrella
# -----------------------------------------------------
print("Paso B: Aplicando transformaciones y normalización (Esquema Estrella)...")

# 1. Limpieza de nombres de columnas a snake_case
def clean_col_name(col):
    col = col.lower().replace(' ', '_').replace('.', '').replace('-', '_')
    col = col.replace('total_esg_risk_score', 'esg_risk_score')
    col = col.replace('environment_risk_score', 'env_risk_score')
    col = col.replace('governance_risk_score', 'gov_risk_score')
    col = col.replace('social_risk_score', 'soc_risk_score')
    col = col.replace('esg_risk_percentile', 'esg_percentile')
    col = col.replace('esg_risk_level', 'esg_level')
    return col

df_fact.columns = [clean_col_name(col) for col in df_fact.columns]
column_names = df_fact.columns.tolist()

# 2. Definición de Tipos y Limpieza
numeric_cols = [
    'full_time_employees',
    'esg_risk_score',
    'env_risk_score',
    'gov_risk_score',
    'soc_risk_score',
    'controversy_score',
    'esg_percentile'
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
# Generar un ID único autoincremental
df_fact = df_fact.sort_values(by='symbol').reset_index(drop=True)
df_fact['risk_id'] = df_fact.index + 1

# Selección final de columnas para la TABLA DE HECHOS
fact_columns = [
    'risk_id',                 # Primary Key
    'symbol',                  # Clave para relacionar con fact_company_info (capa Gold)
    'name',                    # Nombre de la compañía
    'full_time_employees',     # Métrica/Atributo
    'esg_risk_score',          # Métrica
    'env_risk_score',          # Métrica
    'gov_risk_score',          # Métrica
    'soc_risk_score',          # Métrica
    'controversy_score',       # Métrica
    'esg_percentile',          # Métrica
    'sector_id',               # Foreign Key
    'industry_id',             # Foreign Key
    'esg_level_id',            # Foreign Key
    'controversy_level_id'     # Foreign Key
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
    (df_dim_sector, DIM_SECTOR_NAME),
    (df_dim_industry, DIM_INDUSTRY_NAME),
    (df_dim_esg, DIM_ESG_LEVEL_NAME),
    (df_dim_controv, DIM_CONTROV_NAME),
    (df_fact_table, FACT_RISK_NAME), 
]

try:
    with engine.begin() as connection:
        
        # Limpiando tablas existentes (DROP CASCADE) para evitar errores de dependencia
        print("Limpiando tablas existentes (DROP CASCADE)...")
        connection.execute(text(f"DROP TABLE IF EXISTS {FACT_RISK_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_SECTOR_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_INDUSTRY_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_ESG_LEVEL_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_CONTROV_NAME} CASCADE;"))
        print("Limpieza completada.")


        # 1. Cargar las tablas de Dimensión
        for df, table_name in tables_to_load[:-1]:
             df.to_sql(table_name, connection, if_exists="replace", index=False)
             print(f"✔ Dimensión '{table_name}' cargada.")

        # 2. Cargar la Tabla de Hechos
        df_fact_table.to_sql(FACT_RISK_NAME, connection, if_exists="replace", index=False)
        print(f"✔ Tabla de Hechos '{FACT_RISK_NAME}' cargada.")

        # Establecer PRIMARY KEYs
        print("Estableciendo claves primarias...")
        connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD PRIMARY KEY (risk_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_SECTOR_NAME} ADD PRIMARY KEY (sector_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_INDUSTRY_NAME} ADD PRIMARY KEY (industry_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_ESG_LEVEL_NAME} ADD PRIMARY KEY (esg_level_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_CONTROV_NAME} ADD PRIMARY KEY (controversy_level_id);"))
        
        # Establecer FOREIGN KEYs
        print("Estableciendo claves foráneas...")
        connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (sector_id) REFERENCES {DIM_SECTOR_NAME} (sector_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (industry_id) REFERENCES {DIM_INDUSTRY_NAME} (industry_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (esg_level_id) REFERENCES {DIM_ESG_LEVEL_NAME} (esg_level_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_RISK_NAME} ADD FOREIGN KEY (controversy_level_id) REFERENCES {DIM_CONTROV_NAME} (controversy_level_id);"))


    print("🎉 Normalización y Carga completadas para Análisis de Riesgo con Esquema Estrella.")

except Exception as e:
    print(f"❌ Error al cargar a RDS: {e}")