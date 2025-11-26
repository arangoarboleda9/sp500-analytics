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
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = config.AWS_DEFAULT_REGION         
SILVER_BUCKET_NAME = config.S3_BUCKET    
S3_KEY_PATH = "silver/company_historical_stocks/fact_historical_stocks.parquet" 

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
RAW_TABLE_NAME = "company_historical_stocks"
FACT_STOCKS_NAME = "fact_historical_stocks" # Tabla de Hechos
# Nombres de las dimensiones
DIM_DATE_NAME = "dim_stock_date"
DIM_TICKER_NAME = "dim_stock_ticker"


# -----------------------------------------------------
# Conectar a RDS PostgreSQL
# -----------------------------------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

print("Conectado a la base de datos RDS.")

# -----------------------------------------------------
# Paso A: Extracción (E) - Leer datos JSONB desde la capa Raw
# -----------------------------------------------------
print(f"Paso A: Extrayendo datos JSONB de la tabla Raw: {RAW_TABLE_NAME}")

query = f"SELECT data FROM {RAW_TABLE_NAME};"

try:
    df_raw = pd.read_sql(query, engine) 
    # El paso de normalización en el script anterior falló porque la estructura es [{ticker: metrics, ticker2: metrics}, ...]
    # Necesitamos procesar la columna 'data' manualmente.
    data_list = df_raw['data'].tolist()
    
except Exception as e:
    print(f"❌ Error al extraer datos JSONB de RDS: {e}")
    exit()

print(f"Datos extraídos. Número de objetos JSON: {len(data_list)}")

# -----------------------------------------------------
# Paso B: Transformación (T) - DESANIDAR y DESCOMPONER
# -----------------------------------------------------
print("Paso B: Aplicando desanidamiento y descomposición de cadenas (Esquema Estrella)...")

all_records = []
METRIC_COLUMNS = ['open', 'high', 'low', 'close', 'volume', 'unknown_metric1', 'unknown_metric2']

for json_obj in data_list:
    if not json_obj:
        continue

    # 1. Extraer la fecha y eliminarla del objeto para dejar solo los tickers/métricas
    trade_date_str = json_obj.pop('Date', None)
    
    if not trade_date_str:
        continue
        
    # 2. Iterar sobre el resto de las claves (que son los tickers)
    for ticker, metrics_str in json_obj.items():
        if ticker in ['date', 'Date', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']:
            # Ignorar claves de una sola letra o claves obvias que no son tickers (puede haber falsos positivos, pero priorizamos tickers válidos)
            if len(ticker) == 1:
                continue
                
        # 3. Descomponer la cadena de métricas por '//'
        
        # Si el valor es solo "0" o un solo float (como 0.0), no hay métricas completas. Ignorar.
        if isinstance(metrics_str, float) or metrics_str == '0':
             continue
        
        metrics = metrics_str.split('//')
        
        # Asegurar que tenemos al menos el Close (índice 3) para que el registro sea útil
        if len(metrics) >= 4:
            record = {'trade_date': trade_date_str, 'ticker': ticker.upper()}
            
            # Mapear los valores de la cadena a las columnas
            for i, col_name in enumerate(METRIC_COLUMNS):
                if i < len(metrics):
                    record[col_name] = metrics[i]
                
            all_records.append(record)

df_fact = pd.DataFrame(all_records)

# Verificación después del desanidamiento
if df_fact.empty:
    print("❌ ERROR: El desanidamiento no produjo ninguna fila válida. Revisar la estructura de la cadena de métricas.")
    exit()

print(f"Desanidamiento completado. Filas después de la descomposicón: {len(df_fact)}")

# -----------------------------------------------------
# Continuación del Paso B: Limpieza y Normalización
# -----------------------------------------------------

# 1. Conversión de Tipos de Datos Numéricos
numeric_cols = ['open', 'high', 'low', 'close', 'volume']
for col in numeric_cols:
    if col in df_fact.columns:
        # Los valores ya están limpios de $ y , por la división. Solo convertir.
        df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce')

# 2. Conversión de fechas
if 'trade_date' in df_fact.columns:
    # Usar utc=True si la columna es de zona horaria mixta y normalizar a solo la fecha
    df_fact['trade_date'] = pd.to_datetime(df_fact['trade_date'], errors='coerce', utc=True).dt.normalize()
    
# 3. Limpieza de Ticker
if 'ticker' in df_fact.columns:
    df_fact['ticker'] = df_fact['ticker'].astype(str).str.strip().str.upper()

# 4. Eliminar filas sin identificadores clave
df_fact.dropna(subset=['ticker', 'trade_date', 'close'], inplace=True)
df_fact = df_fact.reset_index(drop=True)

# --- NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

# 1. DIM_DATE
df_dim_date = df_fact[['trade_date']].drop_duplicates().dropna().copy().reset_index(drop=True)
df_dim_date['date_id'] = df_dim_date.index + 1
# Extraer atributos de tiempo (usando .dt.date para obtener solo la fecha para el merge)
df_dim_date['date_only'] = df_dim_date['trade_date'].dt.date 
df_dim_date['year'] = df_dim_date['trade_date'].dt.year
df_dim_date['month'] = df_dim_date['trade_date'].dt.month
df_dim_date['day'] = df_dim_date['trade_date'].dt.day

df_dim_date = df_dim_date[['date_id', 'trade_date', 'date_only', 'year', 'month', 'day']]
# Merge usando solo el componente de fecha
df_fact['date_only'] = df_fact['trade_date'].dt.date 
df_fact = pd.merge(df_fact, df_dim_date[['date_id', 'date_only']], on='date_only', how='left')
df_fact.drop(columns=['date_only'], inplace=True) # Limpiar columna auxiliar


# 2. DIM_TICKER (Solo ticker, ya que no hay 'company_name' en este dataset)
df_dim_ticker = df_fact[['ticker']].drop_duplicates().dropna(subset=['ticker']).reset_index(drop=True)
df_dim_ticker['ticker_id'] = df_dim_ticker.index + 1
df_dim_ticker = df_dim_ticker[['ticker_id', 'ticker']]
df_fact = pd.merge(df_fact, df_dim_ticker, on='ticker', how='left')


# 3. CREACIÓN DE LA TABLA DE HECHOS (FACT_HISTORICAL_STOCKS)
df_fact = df_fact.sort_values(by=['ticker', 'trade_date']).reset_index(drop=True)
df_fact['stock_id'] = df_fact.index + 1

# Selección final de columnas para la TABLA DE HECHOS
fact_columns = [
    'stock_id',        # Primary Key
    'date_id',         # Foreign Key a dim_date
    'ticker_id',       # Foreign Key a dim_ticker
    'open',            # Métrica
    'high',            # Métrica
    'low',             # Métrica
    'close',           # Métrica
    'volume',          # Métrica
    # 'unknown_metric1', # Descartamos métricas desconocidas
    # 'unknown_metric2'
]

df_fact_table = df_fact[[col for col in fact_columns if col in df_fact.columns]].copy()
# Asegurar tipos INT para las claves
df_fact_table['date_id'] = df_fact_table['date_id'].astype(np.int64)
df_fact_table['ticker_id'] = df_fact_table['ticker_id'].astype(np.int64)


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
    (df_dim_ticker, DIM_TICKER_NAME),
    (df_fact_table, FACT_STOCKS_NAME), 
]

try:
    with engine.begin() as connection:
        
        # Limpiando tablas existentes (DROP CASCADE) para evitar errores de dependencia
        print("Limpiando tablas existentes (DROP CASCADE)...")
        connection.execute(text(f"DROP TABLE IF EXISTS {FACT_STOCKS_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_DATE_NAME} CASCADE;"))
        connection.execute(text(f"DROP TABLE IF EXISTS {DIM_TICKER_NAME} CASCADE;"))
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
        connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD PRIMARY KEY (stock_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_DATE_NAME} ADD PRIMARY KEY (date_id);"))
        connection.execute(text(f"ALTER TABLE {DIM_TICKER_NAME} ADD PRIMARY KEY (ticker_id);"))
        
        # Establecer FOREIGN KEYs
        connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (date_id) REFERENCES {DIM_DATE_NAME} (date_id);"))
        connection.execute(text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (ticker_id) REFERENCES {DIM_TICKER_NAME} (ticker_id);"))


    print("🎉 Normalización y Carga completadas para Stocks Históricos con Esquema Estrella.")

except Exception as e:
    print(f"❌ Error al cargar a RDS: {e}")