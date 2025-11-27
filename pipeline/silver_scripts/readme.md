# 🚀 Documentación de la Capa Silver (Data Warehouse)

Este documento describe la estructura del **Esquema Estrella** (Star Schema) de la Capa Silver, que se construye en el Data Lake (S3) y el Data Warehouse (RDS PostgreSQL) mediante la ejecución de los scripts ETL (`pipeline/silver_scripts/`).

---

## 1. Estructura del Data Warehouse (Esquema Estrella)

La capa Silver normaliza los datos en Tablas de Hechos (Facts) y Tablas de Dimensión (Dimensions), optimizadas para el análisis y la integridad referencial.

### 1.1. Tablas de Hechos (Fact Tables)

Contienen métricas y claves foráneas que vinculan las métricas al contexto de las dimensiones.

| Tabla de Hechos (RDS) | Contenido Principal | PK (Clave Primaria) | Claves Foráneas (FK) | Script Fuente |
| :--- | :--- | :--- | :--- | :--- |
| **`fact_company_info`** | Información estática, financiera y descriptiva de las compañías (Núcleo de la dimensión empresarial). | `company_id` | `sector_id`, `industry_id`, `location_id`, `exchange_id` | `company_info_silver.py` |
| **`fact_company_reviews`** | Métricas de calificaciones, entrevistas y aprobación de CEO. | `review_id` | `company_id` (a `fact_company_info`), `review_industry_id`, `headquarters_id`, `ceo_approval_id` | `company_reviews_silver.py` |
| **`fact_company_risk`** | Puntuaciones de riesgo ESG (Environmental, Social, Governance) y controversias. | `risk_id` | `sector_id`, `industry_id`, `esg_level_id`, `controversy_level_id` | `company_riesgos_silver.py` |
| **`fact_stock_prices`** | Precios de acciones diarios (data actual). | `stock_price_id` | `date_id`, `symbol_id` | `company_stocks.py` |
| **`fact_historical_stocks`** | Precios históricos de acciones (desde JSONB desanidado). | `stock_id` | `date_id`, `ticker_id` | `historical_stocks_silver.py` |

#### **Columnas Clave en Tablas de Hechos**

| Columna | Descripción | Tablas que la Contienen |
| :--- | :--- | :--- |
| **`company_id`** | Identificador único de la compañía (PK en `info`, FK en `reviews`). | `fact_company_info`, `fact_company_reviews` |
| **`symbol`** | Ticker bursátil de la compañía (ej: MSFT). | `fact_company_info`, `fact_company_risk` |
| **`marketcap`** | Capitalización de mercado. | `fact_company_info` |
| **`rating`** | Calificación promedio de la compañía (Reviews). | `fact_company_reviews` |
| **`esg_risk_score`** | Puntuación total de riesgo ESG (métrica principal). | `fact_company_risk` |
| **`open`, `close`, `volume`** | Precios de apertura, cierre y volumen de negociación diario. | `fact_stock_prices`, `fact_historical_stocks` |

---

### 1.2. Tablas de Dimensión (Dimension Tables)

Proporcionan el contexto para los hechos, evitando redundancia de texto.

| Tabla de Dimensión (RDS) | Clave Primaria (PK) | Contenido de la Dimensión | Script Fuente |
| :--- | :--- | :--- | :--- |
| **`dim_sector`** | `sector_id` | Nombre del sector económico (ej: Technology). | `company_info_silver.py` |
| **`dim_industry`** | `industry_id` | Nombre de la industria específica (ej: Software—Application). | `company_info_silver.py` |
| **`dim_location`** | `location_id` | Ubicación geográfica de la sede principal (Ciudad, Estado, País). | `company_info_silver.py` |
| **`dim_date`** | `date_id` | Desglose temporal (Año, Mes, Día, etc.) para análisis de precios. | `company_stocks.py` / `historical_stocks_silver.py` |
| **`dim_symbol`** | `symbol_id` | Nombres de los tickers de acciones. | `company_stocks.py` |
| **`dim_headquarters`** | `headquarters_id` | Ubicaciones reportadas en el dataset de Reviews. | `company_reviews_silver.py` |
| **`dim_esg_level`** | `esg_level_id` | Niveles discretos de riesgo ESG (Bajo, Medio, Alto). | `company_riesgos_silver.py` |

---

## 2. 🌳 Orden de Ejecución para la Creación del DAG

El DAG debe garantizar que las tablas de las que dependen otras tablas (Tablas de Hechos) sean creadas primero.

### Fases de Ejecución

| Fase | Tareas (Scripts) | Dependencias Clave |
| :--- | :--- | :--- |
| **FASE 1: FUNDACIÓN (Base de la Compañía)** | `company_info_silver.py` | Ninguna. **Crea la clave `company_id` y las dimensiones centrales.** |
| **FASE 2: INDEPENDIENTES (Datos Paralelos)** | `company_index_silver.py` | Ninguna. |
| | `company_stocks.py` | Ninguna. |
| | `historical_stocks_silver.py` | Ninguna. |
| **FASE 3: DEPENDIENTES (Consumo de la Base)** | `company_reviews_silver.py` | **Depende de `company_info_silver.py`** (Usa `INNER JOIN` con `fact_company_info`). |
| | `company_riesgos_silver.py` | **Depende de `company_info_silver.py`** (Por convención para asegurar dimensiones compartidas). |

### 3. 📝 Detalle de la Lógica de Normalización por Script

| Script | Descripción de la Transformación y Carga |
| :--- | :--- |
| `company_info_silver.py` | **Normalización Central:** Normaliza `sector`, `industry`, `location` y `exchange` en dimensiones separadas. La data empresarial se carga en `fact_company_info` (PK: `company_id`). |
| `company_reviews_silver.py` | **JOIN y Normalización:** Efectúa un **JOIN con la tabla `fact_company_info` (T1)** para obtener la FK `company_id`. Normaliza atributos como `headquarters` y `ceo_approval`. |
| `company_riesgos_silver.py` | **Riesgo ESG:** Transforma las puntuaciones de riesgo ESG. Normaliza los niveles de riesgo y controversia en sus dimensiones. La tabla `fact_company_risk` almacena las métricas ESG. |
| `company_stocks.py` | **Hechos de Precio:** Crea las dimensiones `dim_date` y `dim_symbol`. Los valores diarios (`open`, `close`, `volume`) se cargan en `fact_stock_prices`. |
| `historical_stocks_silver.py` | **Desanidación:** Transforma la data histórica anidada (JSONB) en filas planas. Crea sus propias dimensiones de `date` y `ticker`. |
| `company_index_silver.py` | **Limpieza Plana:** Proceso ETL simple de limpieza y carga directa a la tabla `sp500_index_silver`. |


### Estructura de Dependencias del DAG (Airflow, etc.)

Para crear el DAG, las dependencias deben definirse así:

```python
# Tarea 1: La fundación debe ejecutarse primero
T1_INFO = 'company_info_silver.py' 

# Tareas independientes (pueden correr en paralelo o después de T1)
T2_INDEX = 'company_index_silver.py'
T3_STOCKS = 'company_stocks.py'
T4_HISTORICAL = 'historical_stocks_silver.py'

# Tareas dependientes
T5_REVIEWS = 'company_reviews_silver.py'
T6_RIESGOS = 'company_riesgos_silver.py'

# 1. Tareas de Fundación (T1) -> Tareas Dependientes (T5, T6)
T1_INFO >> [T5_REVIEWS, T6_RIESGOS]

# 2. Tareas Independientes (T2, T3, T4) pueden ejecutarse sin restricciones directas
#    o en paralelo con las fases anteriores para eficiencia.

# Estructura de ejecución:
# T1 (INFO) debe terminar para que T5 (REVIEWS) y T6 (RIESGOS) comiencen.

