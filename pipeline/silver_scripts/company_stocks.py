import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import numpy as np 
import boto3
from io import BytesIO 
from config import Config
import config
# -----------------------------------------------------
# ⚠️ CONFIGURACIÓN DE CREDENCIALES AWS y DESTINO S3 ⚠️
# -----------------------------------------------------
# Credenciales de Acceso AWS (INYECCIÓN MANUAL)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = config.AWS_DEFAULT_REGION         
SILVER_BUCKET_NAME = config.S3_BUCKET    
S3_KEY_PATH = "silver/company_stocks/fact_stock_prices.parquet" 

# -----------------------------------------------------
# CONFIGURACIÓN DE RDS Y TABLAS
# -----------------------------------------------------
load_dotenv() 

DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# --- VERIFICACIÓN DE ERROR DE ENTORNO (DB_PORT 'None') ---
if DB_PORT is None or not str(DB_PORT).isdigit(): 
    print("❌ ERROR: La variable DB_PORT no se cargó correctamente o no es un número.")
    print("Asegúrate de que tu archivo .env existe y contiene DB_PORT=\"5432\" (o el puerto correcto).")
    exit()
# ----------------------------------------------------------

# Nombres de las tablas
RAW_TABLE_NAME = "company_stocks" 
FACT_STOCKS_NAME = "fact_stock_prices" # Tabla de Hechos
# Nombres de las dimensiones
DIM_DATE_NAME = "dim_date"
DIM_SYMBOL_NAME = "dim_symbol"


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
df_fact.columns = [col.lower().replace(' ', '_').replace('.', '').replace('-', '_') for col in df_fact.columns]

# 2. Definición de Tipos y Limpieza
numeric_cols = ['adj_close', 'close', 'high', 'low', 'open', 'volume']

# Procesar columnas numéricas
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
# Crear la dimensión de tiempo a partir de las fechas únicas
df_dim_date = df_fact[['date']].drop_duplicates().dropna().copy().reset_index(drop=True)
df_dim_date['date_id'] = df_dim_date.index + 1
df_dim_date['year'] = df_dim_date['date'].dt.year
df_dim_date['month'] = df_dim_date['date'].dt.month
df_dim_date['day'] = df_dim_date['date'].dt.day
df_dim_date['day_of_week'] = df_dim_date['date'].dt.dayofweek # 0=Lunes, 6=Domingo
df_dim_date['quarter'] = df_dim_date['date'].dt.quarter
df_dim_date['is_weekend'] = np.where(df_dim_date['day_of_week'].isin([5, 6]), True, False)

df_dim_date = df_dim_date[['date_id', 'date', 'year', 'month', 'day', 'day_of_week', 'quarter', 'is_weekend']]
df_fact = pd.merge(df_fact, df_dim_date[['date_id', 'date']], on='date', how='left')


# 2. DIM_SYMBOL
# Crear la dimensión de Símbolo (Ticker)
df_dim_symbol = df_fact[['symbol']].drop_duplicates().dropna().reset_index(drop=True)
df_dim_symbol['symbol_id'] = df_dim_symbol.index + 1
df_dim_symbol = df_dim_symbol[['symbol_id', 'symbol']]
df_fact = pd.merge(df_fact, df_dim_symbol, on='symbol', how='left')


# 3. CREACIÓN DE LA TABLA DE HECHOS (FACT_STOCK_PRICES)
# Generar un ID único autoincremental para cada registro de precio
df_fact = df_fact.sort_values(by=['symbol', 'date']).reset_index(drop=True)
df_fact['stock_price_id'] = df_fact.index + 1


# Selección final de columnas para la TABLA DE HECHOS
fact_columns = [
    'stock_price_id',  # Primary Key
    'date_id',         # Foreign Key a dim_date
    'symbol_id',       # Foreign Key a dim_symbol
    'adj_close',       # Métrica
    'close',           # Métrica
    'high',            # Métrica
    'low',             # Métrica
    'open',            # Métrica
    'volume'           # Métrica
]

df_fact_table = df_fact[[col for col in fact_columns if col in df_fact.columns]].copy()
# Asegurar tipos INT para las claves
df_fact_table['date_id'] = df_fact_table['date_id'].astype(np.int64)
df_fact_table['symbol_id'] = df_fact_table['symbol_id'].astype(np.int64)


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
    (df_dim_date, DIM_DATE_NAME),
    (df_dim_symbol, DIM_SYMBOL_NAME),
    (df_fact_table, FACT_STOCKS_NAME), 
]

try:
    with engine.begin() as connection:
        
        # Limpiando tablas existentes (DROP CASCADE) para evitar errores de dependencia
        print("Limpiando tablas existentes (DROP CASCADE)...")
        connection.execute(text(f"DROP TABLE IF EXISTS {FACT_STOCKS_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_DATE_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_SYMBOL_NAME} CASCADE;"))
        print("Limpieza completada.")


        # 1. Cargar las tablas de Dimensión
        for df, table_name in tables_to_load[:-1]:
             df.to_sql(table_name, connection, if_exists="replace", index=False)
             print(f"✔ Dimensión '{table_name}' cargada.")

        # 2. Cargar la Tabla de Hechos
        df_fact_table.to_sql(FACT_STOCKS_NAME, connection, if_exists="replace", index=False)
        print(f"✔ Tabla de Hechos '{FACT_STOCKS_NAME}' cargada.")

        # Establecer PRIMARY KEYs
        print("Estableciendo claves primarias...")
        connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD PRIMARY KEY (stock_price_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_DATE_NAME} ADD PRIMARY KEY (date_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_SYMBOL_NAME} ADD PRIMARY KEY (symbol_id);"))
        
        # Establecer FOREIGN KEYs
        print("Estableciendo claves foráneas...")
        connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (date_id) REFERENCES {DIM_DATE_NAME} (date_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (symbol_id) REFERENCES {DIM_SYMBOL_NAME} (symbol_id);"))


    print("🎉 Normalización y Carga completadas para Precios de Acciones con Esquema Estrella.")

except Exception as e:
    print(f"❌ Error al cargar a RDS: {e}")