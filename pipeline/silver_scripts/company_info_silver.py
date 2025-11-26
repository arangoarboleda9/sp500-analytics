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
# NOTA: La clave S3 sigue apuntando a la tabla principal (Hechos)
S3_KEY_PATH = "silver/company_info/company_info_silver.parquet"

# -----------------------------------------------------
# CONFIGURACIÓN DE RDS Y TABLAS
# -----------------------------------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Nombres de las tablas
RAW_TABLE_NAME = "company_info"
FACT_TABLE_NAME = "fact_company_info" # Nuevo nombre para la tabla de hechos
DIM_SECTOR_NAME = "dim_sector"
DIM_INDUSTRY_NAME = "dim_industry"
DIM_LOCATION_NAME = "dim_location" # Unificando City, State, Country
DIM_EXCHANGE_NAME = "dim_exchange"

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
        df_raw = pd.read_sql(query, connection)
except Exception as e:
    print(f"❌ Error al extraer datos de RDS: {e}")
    exit()

print(f"Datos extraídos. Filas: {len(df_raw)}")

# -----------------------------------------------------
# Paso B: Transformación (T) - Limpieza, Ajustes y NORMALIZACIÓN
# -----------------------------------------------------
print("Paso B: Aplicando transformaciones y normalización (Esquema Estrella)...")

df_silver = df_raw.copy()

# 1. Limpieza de nombres de columnas
df_silver.columns = [col.lower().replace(' ', '_').replace('.', '').replace('-', '_') for col in df_silver.columns]

# 💡 CORRECCIÓN PYlance: Obtener los nombres de columna como lista para evitar advertencias
column_names = df_silver.columns.tolist()

# 2. Definición de Tipos y Limpieza de Texto por columna
numeric_cols = ['currentprice', 'marketcap', 'ebitda', 'revenuegrowth', 'fulltimeemployees', 'weight']

# Procesar columnas numéricas
for col in numeric_cols:
    if col in df_silver.columns:
        df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')

# 💡 LIMPIEZA DE TEXTO ESPECÍFICA Y ESTANDARIZACIÓN
for col in column_names:

    if col not in df_silver.columns:
        continue

    # Aseguramos que las columnas de texto sean strings y limpiamos espacios
    if df_silver[col].dtype == 'object' or col in ['symbol', 'shortname', 'longname', 'sector', 'industry', 'city', 'state', 'country', 'longbusinesssummary', 'exchange']:
        df_silver[col] = df_silver[col].astype(str).str.strip()

    # Limpieza específica para EXCHANGE (Mantener UPPERCASE)
    if col == 'exchange':
        df_silver[col] = df_silver[col].str.upper()

    # Limpieza específica para campos descriptivos (Capitalizar)
    elif col in ['sector', 'industry', 'city', 'state', 'country', 'longname', 'shortname']:
        df_silver[col] = df_silver[col].str.title()

# 3. Eliminar filas con información crítica faltante
df_silver.dropna(subset=['symbol'], inplace=True)


# --- PASO DE NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

# 1. DIM_SECTOR
df_dim_sector = df_silver[['sector']].drop_duplicates().reset_index(drop=True)
df_dim_sector['sector_id'] = df_dim_sector.index + 1
df_dim_sector = df_dim_sector[['sector_id', 'sector']]
df_silver = pd.merge(df_silver, df_dim_sector, on='sector', how='left')

# 2. DIM_INDUSTRY
df_dim_industry = df_silver[['industry']].drop_duplicates().reset_index(drop=True)
df_dim_industry['industry_id'] = df_dim_industry.index + 1
df_dim_industry = df_dim_industry[['industry_id', 'industry']]
df_silver = pd.merge(df_silver, df_dim_industry, on='industry', how='left')

# 3. DIM_LOCATION (City, State, Country)
df_dim_location = df_silver[['city', 'state', 'country']].drop_duplicates().reset_index(drop=True)
df_dim_location['location_id'] = df_dim_location.index + 1
df_dim_location = df_dim_location[['location_id', 'city', 'state', 'country']]
df_silver = pd.merge(df_silver, df_dim_location, on=['city', 'state', 'country'], how='left')

# 4. DIM_EXCHANGE
# NOTA: Asumimos que 'exchange' fue limpiado antes
df_dim_exchange = df_silver[['exchange']].drop_duplicates().reset_index(drop=True)
df_dim_exchange['exchange_id'] = df_dim_exchange.index + 1
df_dim_exchange = df_dim_exchange[['exchange_id', 'exchange']]
df_silver = pd.merge(df_silver, df_dim_exchange, on='exchange', how='left')


# 5. CREACIÓN DE LA TABLA DE HECHOS (FACT_COMPANY_INFO)
# Generar un ID único autoincremental para la compañía (Hecho)
df_silver['company_id'] = df_silver.reset_index().index + 1

# Selección final de columnas para la TABLA DE HECHOS
fact_columns = [
    'company_id', # Primary Key / ID Autoincremental
    'symbol',
    'shortname',
    'longbusinesssummary', # Atributos de la compañía que NO forman parte de una dimensión
    'currentprice',
    'marketcap',
    'ebitda',
    'revenuegrowth',
    'fulltimeemployees',
    'weight',
    'sector_id',     # Foreign Key
    'industry_id',   # Foreign Key
    'location_id',   # Foreign Key
    'exchange_id'    # Foreign Key
]

df_fact_table = df_silver[[col for col in fact_columns if col in df_silver.columns]]


print(f"DataFrame de Hechos listo. Filas: {len(df_fact_table)}")
print("Tipos de datos finales de la Tabla de Hechos:")
print(df_fact_table.dtypes)
print("\nDimensiones creadas:")
print(f"Dim_Sector: {len(df_dim_sector)} filas")
print(f"Dim_Industry: {len(df_dim_industry)} filas")
print(f"Dim_Location: {len(df_dim_location)} filas")
print(f"Dim_Exchange: {len(df_dim_exchange)} filas")

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
    # Cargamos la tabla de hechos resultante a S3
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

# -----------------------------------------------------
# Paso D: Carga a RDS (Data Warehouse/Análisis - Carga Secundaria)
# -----------------------------------------------------
print(f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella")

# Definir la lista de DataFrames y sus nombres de tabla
tables_to_load = [
    (df_dim_sector, DIM_SECTOR_NAME),
    (df_dim_industry, DIM_INDUSTRY_NAME),
    (df_dim_location, DIM_LOCATION_NAME),
    (df_dim_exchange, DIM_EXCHANGE_NAME),
    (df_fact_table, FACT_TABLE_NAME), # La tabla de Hechos va al final
]

try:
    with engine.begin() as connection:
        # 1. Cargar las tablas de Dimensión (se hace REPLACE para simplificar)
        for df, table_name in tables_to_load[:-1]: # Excluir la tabla de hechos por ahora
             df.to_sql(table_name, connection, if_exists="replace", index=False)
             print(f"✔ Dimensión '{table_name}' cargada.")

        # 2. Cargar la Tabla de Hechos
        df_fact_table.to_sql(FACT_TABLE_NAME, connection, if_exists="replace", index=False)
        print(f"✔ Tabla de Hechos '{FACT_TABLE_NAME}' cargada.")

        # Opcional: Establecer PRIMARY KEYs y FOREIGN KEYs para la integridad
        print("Estableciendo claves (Opcional)...")
        connection.execute(text(f"ALTER TABLE {DIM_SECTOR_NAME} ADD PRIMARY KEY (sector_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_INDUSTRY_NAME} ADD PRIMARY KEY (industry_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_LOCATION_NAME} ADD PRIMARY KEY (location_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_EXCHANGE_NAME} ADD PRIMARY KEY (exchange_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD PRIMARY KEY (company_id);"))

        connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (sector_id) REFERENCES {DIM_SECTOR_NAME} (sector_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (industry_id) REFERENCES {DIM_INDUSTRY_NAME} (industry_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (location_id) REFERENCES {DIM_LOCATION_NAME} (location_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_TABLE_NAME} ADD FOREIGN KEY (exchange_id) REFERENCES {DIM_EXCHANGE_NAME} (exchange_id);"))


    print("🎉 Normalización y Carga a RDS completadas con Esquema Estrella.")

except Exception as e:
    print(f"❌ Error al cargar a RDS: {e}")