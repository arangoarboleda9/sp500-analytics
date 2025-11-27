# Documento de Requerimientos de la Solución — Proyecto S&P 500 Analytics
Proyecto: SP500 Analytics

## Índice

- [1. Contexto](#1-contexto)  
- [2. Objetivo general](#2-objetivo-general)  
- [3. Preguntas de negocio que debe resolver](#3-preguntas-de-negocio-que-debe-resolver)  
- [4. Alcance del sistema](#4-alcance-del-sistema)  
- [5. Requerimientos funcionales](#5-requerimientos-funcionales)  
- [6. Requerimientos no funcionales](#6-requerimientos-no-funcionales)  
- [7. Datasets requeridos](#7-datasets-requeridos)  
- [8. Infraestructura necesaria](#8-infraestructura-necesaria)  
    - [8.1 Compute](#81-compute)  
    - [8.2 Storage](#82-storage)  
    - [8.3 IaC](#83-iac)  
- [9. Pipeline requerido](#9-pipeline-requerido)  
- [10. Requerimientos de calidad de datos](#10-requerimientos-de-calidad-de-datos)  
- [12. Propuesta de ingesta continua](#12-propuesta-de-ingesta-continua)  
- [13. Stakeholders](#13-stakeholders)

## 1. Contexto

El acceso a información financiera clara, actualizada y comprensible no es universal.
Muchas personas interesadas en invertir no cuentan con herramientas accesibles para analizar datos del mercado bursátil.

Este proyecto busca construir una plataforma de análisis del índice S&P 500, permitiendo a cualquier usuario comprender mejor el comportamiento del mercado mediante datos confiables y modelos analíticos.

## 2. Objetivo general

Desarrollar una arquitectura de datos moderna (cloud + data lakehouse) que permita:

- Ingerir datasets del S&P 500 desde fuentes públicas (Kaggle / AWS).

- Procesarlos mediante un pipeline batch y preparar una capa analítica confiable.

- Exponer indicadores que respondan preguntas clave sobre el mercado.


## 3. Preguntas de negocio que debe resolver

Comportamiento del índice

- ¿Cómo ha evolucionado el valor del S&P 500 a lo largo del tiempo?

- ¿Cuáles fueron las mayores subas y bajas históricas?

Análisis por empresa

- ¿Qué empresas tienen mejor performance histórica?

- ¿Qué sectores del S&P 500 son los más volátiles?

Comparativas y riesgo

- ¿Cómo se compara la volatilidad entre diferentes empresas?

- ¿Qué empresas muestran mayor correlación con el índice general?


## 4. Alcance del sistema
Incluye

- Ingesta batch desde S3
- Transformaciones bronce → silver
- Implementación de pipeline ETL/ELT con Airflow
- Almacenamiento en AWS (S3 + RDS)
- Infraestructura IaC con Terraform
- Contenedores y orquestación con Docker Compose
- CICD mínimo con Github Actions

## 5. Requerimientos funcionales

RF1 — Ingerir archivos CSV desde un bucket S3
RF2 — Almacenar los datos crudos en la capa RAW (bronze)
RF3 — Transformarlos a formato tabular estandarizado en la capa SILVER
RF4 — Validar la calidad de datos durante la ingesta (duplicados, nulos, tipos)
RF5 — Permitir re-ejecución idempotente del pipeline
RF6 — Registrar logs de cada etapa con timestamps y estados
RF7 — Permitir la ingesta de nuevos archivos si aparecen (propuesta conceptual)

## 6. Requerimientos no funcionales

RNF1 — La solución debe ser modular, extensible y mantenible
RNF2 — Infraestructura reproducible mediante Terraform
RNF3 — Versionado de código y entornos reproductibles vía Docker
RNF4 — Cumplimiento de estándares de estilo: black, isort, flake8 (WIP)
RNF5 — Separación clara entre:

- infraestructura

- app/pipelines

- documentación

RNF6 — Buenas prácticas en S3: particionamiento y compresión (parquet + snappy)

## 7. Datasets requeridos

| Dataset                 | Contenido                               | Tipo      |
| ----------------------- | --------------------------------------- | --------- |
| Historical Stocks       | Precios diarios de empresas del S&P 500 | Raw JSONB |
| SP500 Index             | Valor diario del índice SP500           | Raw CSV   |
| Fundamentals (opcional) | Información financiera de empresas      | Raw CSV   |

**Todos los datasets serán alojados en S3 en la carpeta raw/.**

## 8. Infraestructura necesaria
### 8.1 Compute

- Airflow (Docker Compose)

- Python 3.10


### 8.2 Storage

- S3: data lake (bronze/silver)

- RDS PostgreSQL: almacenamiento raw para staging sql-friendly

### 8.3 IaC

- Terraform para:

    - Crear bucket S3

    - Crear cluster RDS

- Configurar roles IAM

- Security groups

## 9. Pipeline requerido
__1: Ingesta__

- Batch (manual o programada)

- Extrae CSV desde S3

- Valida datos básicos

- Guarda en RDS (para bronze)

- Guarda versión cruda en S3

__2: Bronze → Silver__

Estandarización de:

- tipos de datos

- nombres de columnas

- formatos de fecha

- Deduplicación

- Manejo de outliers

- Exportación final a S3 en parquet particionado

## 10 . Requerimientos de calidad de datos

- DQ1: No permitir fechas nulas en datos históricos

- DQ2: No permitir valores negativos en precios

- DQ3: Duplicate rows check

- DQ4: Validar schema esperado

- DQ5: Verificar unicidad de índice + fecha por empresa


## 12. Propuesta de ingesta continua

Si nuevos archivos aparecen en S3 en la carpeta raw/:

- activar un sensor S3 en Airflow (S3KeySensor)

- disparar el pipeline bronze para el archivo nuevo

- appendear datos a silver

__No se implementa — solo se documenta.__

## 13. Stakeholders

- Equipo de Data Engineering

- Equipo de análisis financiero

- Usuarios interesados en educación financiera