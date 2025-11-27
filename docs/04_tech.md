# Documento Técnico – SP500 Analytics Pipeline

Este documento describe en detalle la arquitectura, los componentes técnicos, las decisiones de diseño y el flujo operativo del pipeline de datos desarrollado para el proyecto **SP500 Analytics**.  
Complementa al README general, proporcionando una especificación técnica orientada a ingeniería de datos.

---

## Índice

- [1. Introducción](#1-introducción)  
- [2. Arquitectura General](#2-arquitectura-general)  
- [3. Diagrama de Arquitectura](#3-diagrama-de-arquitectura)  
- [4. Componentes del sistema](#4-componentes-del-sistema)  
   - [4.1 Orquestación – Apache Airflow](#41-orquestación--apache-airflow)  
   - [4.2 Data Lake – S3 / MinIO](#42-data-lake--s3--minio)  
   - [4.3 Base de Datos – PostgreSQL / Aurora](#43-base-de-datos--postgresql--aurora)  
- [5. Pipeline Detallado](#5-pipeline-detallado)  
   - [5.1 Ingesta](#51-ingesta)  
   - [5.2 Transformación](#52-transformación)  
- [6. Lineamientos de calidad (WIP)](#6-lineamientos-de-calidad-wip)  
- [7. Infraestructura](#7-infraestructura)  
   - [7.1 Local](#71-local)  
   - [7.2 Cloud](#72-cloud)  
- [8. Futuras extensiones](#8-futuras-extensiones)  
- [9. Repositorio](#9-repositorio)

---

# 1. Introducción

El propósito del pipeline es capturar, almacenar, transformar y modelar datos relacionados con el índice S&P 500.  
El enfoque está basado en buenas prácticas de arquitectura de datos modernas, aplicando conceptos de Data Lake, ETL/ELT, automatización con Airflow y modelado semántico con DBT.

---

# 2. Arquitectura General

La arquitectura del proyecto sigue una estructura por capas:

1. **Ingesta (Bronze):**  
    Datos crudos provenientes de Kaggle u otra API, almacenados sin modificaciones.

2. **Transformación (Silver):**  
    Limpieza, normalización y enriquecimiento.  
    Las tablas se cargan en PostgreSQL (o Aurora/Redshift en AWS).

3. **Modelado Semántico (Gold – WIP):**  
    Construcción de métricas, KPIs y modelos dimensionales mediante DBT.

4. **Consumo:**  
    Los datos se exponen para dashboards, notebooks o herramientas analíticas.

---

# 3. Diagrama de Arquitectura

El pipeline sigue el flujo:

S3/MinIO → Airflow → PostgreSQL/Aurora → DBT → Modelos Finales → Dashboards


Diagrama simplificado (Mermaid):

![Diagrama de Arquitectura](/assets/diagram.svg)

# 4. Componentes del sistema

## 4.1 Orquestación – Apache Airflow

Airflow administra la ejecución de:

- Ingesta de archivos CSV/API

- Limpieza y validación

- Cargas a la base de datos

- Transformaciones

Cada DAG está modularizado y dividido en tareas:


| DAG   | Propósito                                |
| ----- | ---------------------------------------- |
| DAG 1 | Descarga datos del S&P500 a Raw          |
| DAG 2 | Procesa CSV crudo y carga a RDS (Bronze) |
| DAG 3 | Transforma Bronze → Silver               |
| DAG 4 | (Gold) – WIP                             |


## 4.2 Data Lake – S3 / MinIO

Estructura:

s3://sp500-bronze/
s3://sp500-silver/
s3://sp500-gold/


### Propósito de cada capa:

- Bronze => Datos crudos sin modificaciones. Estructura similar a la fuente.

- Silver => Datos limpios.
      -   Tipos corregidos.
      -   Columnas estandarizadas.
      -   Normalización (por ejemplo, separar tablas de empresas e índices).

- Gold (WIP)
      - Tablas de hechos: prices, returns, companies_performance.
      - Dimensiones: companies, sectors, calendars.
      - Métricas: tendencias, volatilidad, crecimiento, comparativas.

## 4.3 Base de Datos – PostgreSQL / Aurora

La zona Silver se replica en SQL:

Mejora la capacidad de consulta.

Permite integraciones con DBT.

Centraliza las transformaciones intermedias.

- Tablas típicas en Bronze:

company_info
historical
company_reviews

# 5. Pipeline Detallado
## 5.1 Ingesta

### Tecnologías:

- kagglehub
- Peticiones a APIs
- Lectura y validación
- Salida: archivos crudos en Raw/Bronze.

## 5.2 Transformación

- Limpieza de valores faltantes
- Conversión de strings a fechas
- Transformación de tipos numéricos
- Normalización de columnas
- 


## 6. Lineamientos de calidad (WIP)

En una etapa posterior se agregarán:

Tests de datos: 

- Unicidad
- No nulos
- Relaciones
Validaciones de esquema:

- Campos obligatorios
- Tipos correctos

Monitoreo:

- Logs históricos en Airflow

- Métricas de ejecución

## 7. Infraestructura
### 7.1 Local

- Docker compose
- MinIO
- Airflow (webserver, scheduler, worker, triggerer)
- PostgreSQL

Python scripts en scripts/

## 7.2 Cloud

- S3
- Airflow en ECS 
- Aurora PostgreSQL
- IAM
-  GitHub Actions para automatizar DBT

## 8. Futuras extensiones

- Modelos predictivos (LSTM, Prophet)
- Enriquecimiento con datos de commodities
- Alerts y monitoreo

## 9. Repositorio

Código fuente: https://github.com/LucianaCHA/sp500-analytics