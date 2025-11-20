Preguntas de negocio 

¿Cuáles fueron los períodos de mayor crecimiento y de mayor caída del índice?

¿Qué tan conveniente es comprar o vender una acción en un momento determinado?

¿De qué manera las tendencias de los commodities afectan el desempeño de las empresas?

¿Cómo se comporta el índice general y qué patrones indican tendencia alcista o bajista?

¿Qué impacto tuvo la volatilidad asociada a eventos de crisis (pandemia, guerras, recesiones, etc.)?

¿Qué empresas reportaron crecimiento sostenido durante un período específico?

¿Cuáles fueron las empresas con mayor crecimiento en los últimos X años?

Proceso basico 

1. Ingesta (Raw)

- Fuente: datasets Kaggle

- Descarga automática con scripts / Airflow

- Almacenamiento sin modificaciones (CSV/Parquet)

->>> Destino: raw/ (local o S3)

2. Procesamiento / Limpieza (Clean)

- Limpi- eza básica, normalización, tipos de datos

- Eliminación de columnas irrelevantes

- Manejo de nulos y otros 

- Formato optimizado (Parquet)

 ->>>> Destino: clean/

3. Modelado analítico (Curated / DBT)

- staging: estandarización 1:1 con origen

-intermediate: joins, cálculos, agregaciones

-marts: tablas finales (fact + dims)

- Tests + documentación

4. Almacenamiento final / Capa de consumo

- Warehouse SQL  

- Archivos exportables

- Acceso para analistas, BI o notebooks 

5. Visualización

- Dashboard (PowerBI / Superset / Metabase)

- KPIs, tendencias, comparativos y alertas

6. Orquestación

- Airflow(ingesta,curado, dbt, outputs)

