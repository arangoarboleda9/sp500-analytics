# SP500 Analytics Pipeline

## Índice
- [Objetivo del proyecto](#objetivo-del-proyecto)
- [Flujo general del pipeline (WIP)](#flujo-general-del-pipeline-wip)
- [Arquitectura técnica](#arquitectura-técnica)
- [Arquitectura AWS (WIP)](#arquitectura-aws-wip)
  - [Servicios](#servicios)
  - [Infraestructura como Código (Terraform) - WIP](#infraestructura-como-código-terraform---wip)
- [Estructura general del proyecto](#estructura-general-del-proyecto)
- [Documentación ampliada](#documentación-ampliada)
- [Requisitos](#requisitos)
- [Instalación y uso (versión local de desarrollo)](#instalación-y-uso-versión-local-de-desarrollo)
  - [1. Variables de entorno (.env)](#1-variables-de-entorno-env)
  - [2. Levantar el entorno con Docker](#2-levantar-el-entorno-con-docker)

Este proyecto implementa un pipeline de datos completo para la captura, procesamiento, modelado y análisis de indicadores del índice S&P500. Su objetivo es ofrecer datos confiables y accesibles para la investigación financiera, educación en inversiones y análisis de mercado.

## Objetivo del proyecto

El propósito del pipeline es obtener datos históricos y actuales del S&P500, procesarlos siguiendo buenas prácticas de ingeniería de datos y disponibilizarlos de manera estructurada para su consumo analítico mediante dashboards, consultas SQL o modelos de análisis.

# Flujo general del pipeline (WIP)

* Ingesta de datos del índice S&P500 desde Kaggle

* Almacenamiento sin procesar en la zona raw

* Transformaciones con PySpark o Pandas

* Generación de datasets limpios y normalizados en la zona clean

* Modelado semántico con DBT en la zona curated

* Exposición a dashboards y consultas analíticas

## Arquitectura técnica

El pipeline está basado en una arquitectura modular basada en los pasos del pipeline, para facilitar la escalabilidad y el mantenimiento.

| Componente             | Tecnología / Servicio                       |
| ---------------------- | -------------------------------------------- |
| Orquestación           | Apache Airflow                               |
| Ingesta                | KaggleHub / Python                           |
| Data Lake              | S3 o MinIO (Bronze, Silver, Gold)            |
| Procesamiento          | Pandas / PySpark                             |
| Data Warehouse         | PostgreSQL (local) / Redshift / DuckDB       |
| Visualización          | PowerBI / Superset / Metabase                |

## Arquitectura AWS (WIP)

Esta sección describe la infraestructura en entorno cloud.

### Servicios

- Amazon S3
  - Zona Bronze: sp500-datatsets
  - Zona Silver
  - Zona Gold

- Airflow en EC2/ECS
  Para orquestar las tareas del pipeline.

- Amazon RDS PostgreSQL
  Para la capa silver, analítica y consumo final.

- IAM
  Roles, políticas y permisos para S3, Airflow, etc.

### Infraestructura como Código (Terraform) - WIP

- Implementación de infraestructura (DevOps / IaC)

- Toda la infraestructura cloud del proyecto fue desplegada utilizando Terraform, siguiendo el principio de Infraestructura como Código, garantizando reproducibilidad, trazabilidad y versionamiento controlado dentro del repositorio.

Arquitectura provisionada mediante Terraform

- Los siguientes recursos se crean automáticamente:

 - 1. - Amazon S3 – Data Lake (raw, silver, gold)

      - Bucket único con carpetas por capas

      - Versioning activado

      - Encriptación SSE-AES256

      - Bloqueo total de acceso público


- 2. - EC2 para Airflow

     - Instancia optimizada dentro del Free Tier

     - Security Group restringido (solo 22 y 8080)

     - Elastic IP asociada automáticamente

     - User data para inicializar Docker

- 3. - IAM Roles y Permisos

     - Rol para EC2 con permisos mínimos (principio de mínimos privilegios)

     - Políticas S3 estrictas: GetObject, PutObject, ListBucket

     - Sin credenciales hardcodeadas

- 4. - VPC y red

     - Subred pública

     - Internet Gateway

     - Route table y asociaciones

     - Seguridad aplicada por capa

- Estructura del proyecto Terraform
infra/
└── terraform/
    ├── main.tf
    ├── provider.tf
    ├── variables.tf
    ├── outputs.tf
    ├── network.tf
    ├── compute.tf
    ├── s3_datalake.tf
    ├── iam.tf

 - Comandos para desplegar la infraestructura
     - terraform init
     - terraform fmt
     - terraform validate
     - terraform plan
     - terraform apply

Buenas prácticas aplicadas

Infraestructura modular y escalable

Repositorio organizado e íntegramente versionado

Uso responsable del Free Tier

Seguridad aplicada en todas las capas

Arquitectura reproducible en cualquier entorno

## Estructura general del proyecto
```
sp500-analytics/
├── airflow/
│ ├── dags/
│ ├── logs/
│ └── plugins/
├── assets/
├── data/ # datos locales para desarrollo/test idealmente no sube a git
│   ├── raw/
│   ├── clean/
│   └── curated/
├── dashboard/ # tablero
│   └── wip/
├── docs/
│   ├── 01_project_requiriments.md
│   ├── ... documentación del proyecto
├── minio/
│ └── data/
├── scripts/
├── notebook/
├── infra/ # config de servicios y docker
├── pipeline/
│   ├── bronze/
│   │       ├── scripts/
│   │       ├── ...archivos requeridos para el proceso en esta capa
│   ├── silver/
│   └── gold/
│   └── utils
├── tests/
├── .gitignore
├── README.md
└── requirements.txt

```

## Documentación ampliada
- [`Documento técnico`](docs/04_tech.md)

## Requisitos

- Python 3.10+
- Docker / Docker Compose
- DBT
- AWS CLI (si se usa S3 real)
- Kaggle API configurada

## Desarrollo

### Pre-commit hooks

Este proyecto utiliza pre-commit para mantener la calidad del código. Los hooks se ejecutarán automáticamente antes de cada commit.

**Instalación:**
```bash
pip install -r requirements-dev.txt
pre-commit install
```

O solo pre-commit:
```bash
pip install pre-commit
pre-commit install
```

**Herramientas configuradas:**
- `flake8`: Linting de código Python
- `isort`: Ordenamiento automático de imports
- `black`: Formateo automático de código
- `bandit`: Detección de problemas de seguridad
- Varias verificaciones adicionales (YAML, JSON, trailing whitespace, etc.)

Para más detalles, ver [.pre-commit-setup.md](.pre-commit-setup.md)

**Nota:** Pre-commit está configurado para escanear solo los próximos commits, no modificará archivos existentes en el repositorio.

## Instalación y uso (versión local de desarrollo)

### 1. Variables de entorno (.env)

Todas las configuraciones del entorno local se manejan mediante el archivo `.env` en la raíz.

Ejemplo:
```
AWS_ACCESS_KEY_ID=admin
```
## 2. Levantar el entorno con Docker

```sh
docker compose -p sp-500 up --build
```

Servicios disponibles:

| Servicio | URL | Credenciales |
|---------|-----|--------------|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / admin123 |
| PostgreSQL | localhost:5432 | admin / admin |
