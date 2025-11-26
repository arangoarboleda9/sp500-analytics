# SP500 Analytics Pipeline

Este proyecto implementa un pipeline de datos completo para la captura, procesamiento, modelado y análisis de indicadores del índice S&P500. Su objetivo es ofrecer datos confiables y accesibles para la investigación financiera, educación en inversiones y análisis de mercado.

## Objetivo del proyecto

El propósito del pipeline es obtener datos históricos y actuales del S&P500, procesarlos siguiendo buenas prácticas de ingeniería de datos y disponibilizarlos de manera estructurada para su consumo analítico mediante dashboards, consultas SQL o modelos de análisis.

# Flujo general del pipeline (WIP)

* Ingesta de datos del índice S&P500 desde Kaggle u otra fuente

* Almacenamiento sin procesar en la zona raw

* Transformaciones con PySpark o Pandas

* Generación de datasets limpios y normalizados en la zona clean

* Modelado semántico con DBT en la zona curated

* Exposición a dashboards y consultas analíticas

## Arquitectura técnica

El pipeline está basado en una arquitectura modular basada en los pasos del pipeline, para facilitar la escalabilidad y el mantenimiento.

| Componente            | Tecnología                                   |
| --------------------- | -------------------------------------------- |
| Orquestación          | Apache Airflow                               |
| Ingesta               | Kaggle API / Python                          |
| Almacenamiento bruto  | Amazon S3 (raw zone)                         |
| Procesamiento         | PySpark / Pandas                             |
| Almacenamiento curado | S3 (curated zone)                            |
| Modelado semántico    | DBT                                          |
| Data Warehouse        | Postgres / DuckDB / Redshift (según entorno) |
| Visualización         | PowerBI / Superset / Metabase                |




## Estructura general del proyecto
```
sp500-analytics/
│
├── dags/  # DAGs de Airflow (ingesta, pipelines, automatización)
├── data/ # datos locales para desarrollo/test idealmente no sube a git
│   ├── raw/
│   ├── clean/
│   └── curated/
├── dbt/  # proyecto dbt 
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── docs/
│   ├── tecnico.md
│   ├── arquitectura.png
│   ├── modelo_datos.png
│   └── pipeline_diagrama.png
├── scripts/
├── notebook/               
├── infra/ # Infraestructura (Terraform, Docker, configuración de servicios)
    └── terraform/ # Infraestructura como código (IaC) en AWS
├── dashboard/ # tablero
├── tests/
├── .gitignore
├── README.md
└── requirements.txt

```

## Documentación ampliada 
- [`Documento técnico`](docs/tech.md)

## Requisitos

- Python 3.10+

- Docker / Docker Compose

- AWS CLI configurado (cuando se utilice S3 real)

- DBT

- Airflow

## Instalación y uso (versión local de desarrollo)