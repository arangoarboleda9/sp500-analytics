import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import numpy as np
import boto3
from io import BytesIO
import config
from config import Config
from typing import Tuple, List, Dict, Any

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
S3_KEY_PATH = "silver/company_historical_stocks/fact_historical_stocks.parquet"

# Credenciales de RDS (Inyectadas directamente desde el módulo config)
DB_USER = config.AWS_DB_USER
DB_PASSWORD = config.AWS_DB_PASSWORD
DB_HOST = config.AWS_DB_HOST
DB_PORT = config.AWS_DB_PORT
DB_NAME = config.AWS_DB_NAME

# Nombres de las tablas
RAW_TABLE_NAME = "company_historical_stocks"
FACT_STOCKS_NAME = "fact_historical_stocks"
DIM_DATE_NAME = "dim_stock_date"
DIM_TICKER_NAME = "dim_stock_ticker"

# Definición de las métricas en la cadena '//'
METRIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "unknown_metric1",
    "unknown_metric2",
]


def get_db_engine():
    """Configura y retorna el objeto Engine para la conexión a RDS PostgreSQL."""
    try:
        # Verifica la configuración de la base de datos
        if (
            DB_HOST == config.NOT_SET
            or DB_PORT == config.NOT_SET
            or not str(DB_PORT).isdigit()
        ):
            raise EnvironmentError(
                "Las variables de entorno de RDS no están configuradas correctamente."
            )

        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        print("✔ Conexión a la base de datos RDS establecida.")
        return engine
    except Exception as e:
        print(f"❌ Error al conectar a RDS: {e}")
        return None


def extract_data(engine):
    """Extrae la columna JSONB 'data' de la capa Raw (RDS)."""
    print(f"\nPaso A: Extrayendo datos JSONB de la tabla Raw: {RAW_TABLE_NAME}")

    query = f"SELECT data FROM {RAW_TABLE_NAME};"

    try:
        df_raw = pd.read_sql(text(query), engine)
        data_list = df_raw["data"].tolist()
        print(f"✔ Datos extraídos. Número de objetos JSON: {len(data_list)}")
        return data_list
    except Exception as e:
        print(f"❌ Error al extraer datos JSONB de RDS: {e}")
        return []


def transform_data(
    data_list: List[Dict[str, Any]],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aplica desanidamiento, limpieza, y la normalización del Esquema Estrella."""

    if not data_list:
        print("Lista de datos JSON vacía, omitiendo transformación.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    print("\nPaso B: Aplicando desanidamiento y normalización (Esquema Estrella)...")
    all_records = []

    # --- LÓGICA DE DESANIDAMIENTO Y DESCOMPOSICIÓN DE CADENAS ---
    for json_obj in data_list:
        if not json_obj:
            continue

        trade_date_str = json_obj.pop("Date", None)

        if not trade_date_str:
            continue

        for ticker, metrics_str in json_obj.items():
            # Filtro para ignorar claves cortas o no válidas que no son tickers
            if len(ticker) <= 1:
                continue

            # Ignorar si el valor es un tipo numérico simple (fallo en la fuente de datos)
            if isinstance(metrics_str, float) or metrics_str == "0":
                continue

            metrics = str(metrics_str).split("//")

            # Asegurar que tenemos al menos el Close (índice 3) para que el registro sea útil
            if len(metrics) >= 4:
                record = {"trade_date": trade_date_str, "ticker": ticker.upper()}

                # Mapear los valores de la cadena a las columnas
                for i, col_name in enumerate(METRIC_COLUMNS):
                    if i < len(metrics):
                        record[col_name] = metrics[i]

                all_records.append(record)

    df_fact = pd.DataFrame(all_records)

    if df_fact.empty:
        print("❌ El desanidamiento no produjo ninguna fila válida.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    print(f"Desanidamiento completado. Filas: {len(df_fact)}")

    # 1. Conversión de Tipos de Datos Numéricos
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if col in df_fact.columns:
            df_fact[col] = pd.to_numeric(df_fact[col], errors="coerce")

    # 2. Conversión de fechas
    if "trade_date" in df_fact.columns:
        # Normalizar a solo la fecha (eliminar el componente de tiempo)
        df_fact["trade_date"] = pd.to_datetime(
            df_fact["trade_date"], errors="coerce", utc=True
        ).dt.normalize()

    # 3. Limpieza de Ticker
    if "ticker" in df_fact.columns:
        df_fact["ticker"] = df_fact["ticker"].astype(str).str.strip().str.upper()

    # 4. Eliminar filas sin identificadores clave
    df_fact.dropna(subset=["ticker", "trade_date", "close"], inplace=True)
    df_fact = df_fact.reset_index(drop=True)

    # --- NORMALIZACIÓN (CREACIÓN DE DIMENSIONES) ---

    # 1. DIM_DATE
    df_dim_date = (
        df_fact[["trade_date"]].drop_duplicates().dropna().copy().reset_index(drop=True)
    )
    df_dim_date["date_id"] = df_dim_date.index + 1
    # Extraer atributos de tiempo (usando .dt.date para obtener solo la fecha para el merge)
    df_dim_date["date_only"] = df_dim_date["trade_date"].dt.date
    df_dim_date["year"] = df_dim_date["trade_date"].dt.year
    df_dim_date["month"] = df_dim_date["trade_date"].dt.month
    df_dim_date["day"] = df_dim_date["trade_date"].dt.day

    df_dim_date = df_dim_date[
        ["date_id", "trade_date", "date_only", "year", "month", "day"]
    ]

    # Merge para obtener la clave foránea
    df_fact["date_only"] = df_fact["trade_date"].dt.date
    df_fact = pd.merge(
        df_fact, df_dim_date[["date_id", "date_only"]], on="date_only", how="left"
    )
    df_fact.drop(columns=["date_only"], inplace=True)

    # 2. DIM_TICKER
    df_dim_ticker = (
        df_fact[["ticker"]]
        .drop_duplicates()
        .dropna(subset=["ticker"])
        .reset_index(drop=True)
    )
    df_dim_ticker["ticker_id"] = df_dim_ticker.index + 1
    df_dim_ticker = df_dim_ticker[["ticker_id", "ticker"]]
    df_fact = pd.merge(df_fact, df_dim_ticker, on="ticker", how="left")

    # 3. CREACIÓN DE LA TABLA DE HECHOS (FACT_HISTORICAL_STOCKS)
    df_fact = df_fact.sort_values(by=["ticker", "trade_date"]).reset_index(drop=True)
    df_fact["stock_id"] = df_fact.index + 1

    fact_columns = [
        "stock_id",
        "date_id",
        "ticker_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    df_fact_table = df_fact[
        [col for col in fact_columns if col in df_fact.columns]
    ].copy()
    # Asegurar tipos INT para las claves
    df_fact_table["date_id"] = df_fact_table["date_id"].astype(np.int64)
    df_fact_table["ticker_id"] = df_fact_table["ticker_id"].astype(np.int64)

    print(
        f"✔ Normalización completa. DataFrame de Hechos listo. Filas: {len(df_fact_table)}"
    )

    # Retorna la tabla de hechos y ambas dimensiones
    return df_fact_table, df_dim_date, df_dim_ticker


def load_to_s3(df_fact_table: pd.DataFrame):
    """Carga la Tabla de Hechos a la capa Silver de S3."""
    if df_fact_table.empty:
        print("DataFrame de hechos vacío, omitiendo carga a S3.")
        return

    print(f"\nPaso C: Cargando la Tabla de Hechos en la capa Silver de S3...")

    try:
        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        parquet_buffer = BytesIO()
        df_fact_table.to_parquet(parquet_buffer, index=False)

        s3_client.put_object(
            Bucket=SILVER_BUCKET_NAME, Key=S3_KEY_PATH, Body=parquet_buffer.getvalue()
        )

        print(f"✔ Carga en S3 exitosa: s3://{SILVER_BUCKET_NAME}/{S3_KEY_PATH}")

    except Exception as e:
        print(f"❌ Error al cargar a S3: {e}")


def load_to_rds(
    engine,
    df_fact_table: pd.DataFrame,
    df_dim_date: pd.DataFrame,
    df_dim_ticker: pd.DataFrame,
):
    """Carga Tablas de Dimensión y Hechos a RDS y establece las claves."""
    if df_fact_table.empty or engine is None:
        print("DataFrame o motor de DB no válidos, omitiendo carga a RDS.")
        return

    print(
        f"\nPaso D: Cargando Tablas de Dimensión y Hechos en RDS para Esquema Estrella"
    )

    tables_to_load = [
        (df_dim_date, DIM_DATE_NAME),
        (df_dim_ticker, DIM_TICKER_NAME),
        (df_fact_table, FACT_STOCKS_NAME),
    ]

    try:
        with engine.begin() as connection:

            # Limpiar tablas existentes (DROP CASCADE)
            print("Limpiando tablas existentes (DROP CASCADE)...")
            connection.execute(
                text(f"DROP TABLE IF EXISTS {FACT_STOCKS_NAME} CASCADE;")
            )
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_DATE_NAME} CASCADE;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {DIM_TICKER_NAME} CASCADE;"))
            print("Limpieza completada.")

            # 1. Cargar las tablas de Dimensión y la de Hechos
            for df, table_name in tables_to_load:
                df.to_sql(table_name, connection, if_exists="replace", index=False)
                print(f"✔ Tabla '{table_name}' cargada.")

            # 2. Establecer PRIMARY KEYs
            print("Estableciendo claves primarias...")
            connection.execute(
                text(f"ALTER TABLE {FACT_STOCKS_NAME} ADD PRIMARY KEY (stock_id);")
            )
            connection.execute(
                text(f"ALTER TABLE {DIM_DATE_NAME} ADD PRIMARY KEY (date_id);")
            )
            connection.execute(
                text(f"ALTER TABLE {DIM_TICKER_NAME} ADD PRIMARY KEY (ticker_id);")
            )

            # 3. Establecer FOREIGN KEYs
            print("Estableciendo claves foráneas...")
            connection.execute(
                text(
                    f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (date_id) REFERENCES {DIM_DATE_NAME} (date_id);"
                )
            )
            connection.execute(
                text(
                    f"ALTER TABLE {FACT_STOCKS_NAME} ADD FOREIGN KEY (ticker_id) REFERENCES {DIM_TICKER_NAME} (ticker_id);"
                )
            )

        print(
            "🎉 Normalización y Carga completadas para Stocks Históricos con Esquema Estrella."
        )

    except Exception as e:
        print(f"❌ Error al cargar a RDS o al establecer claves: {e}")


def main():
    """Orquesta la ejecución completa del pipeline ETL."""

    # 1. Obtener motor de DB
    engine = get_db_engine()
    if engine is None:
        return

    # 2. Extracción (Obtiene la lista de objetos JSONB)
    data_list = extract_data(engine)
    if not data_list:
        print("Pipeline finalizado sin datos para procesar.")
        return

    # 3. Transformación y Normalización
    df_fact_table, df_dim_date, df_dim_ticker = transform_data(data_list)

    if df_fact_table.empty:
        print("Pipeline finalizado, no hay datos válidos después de la limpieza.")
        return

    # 4. Carga a S3
    load_to_s3(df_fact_table)

    # 5. Carga a RDS y Establecimiento de Claves
    load_to_rds(engine, df_fact_table, df_dim_date, df_dim_ticker)


if __name__ == "__main__":
    main()
