# Arquitectura del Proyecto


## Descripción General

La arquitectura del proyecto sigue un enfoque moderno de ingeniería de datos basado en capas, lo que permite asegurar calidad, trazabilidad y escalabilidad del pipeline. El flujo va desde la ingesta de datos crudos hasta la generación de modelos analíticos finales listos para responder preguntas de negocio.

---

## 1. Capa Raw – Ingesta de Datos

Los datos provienen de dos fuentes principales:

- **Datasets descargados desde Kaggle**, en formato CSV.

La ingesta es manejada por un DAG dedicado, que almacena toda la información cruda en **Amazon S3**, dentro del directorio:

https://us-east-1.console.aws.amazon.com/s3/buckets/henry-sp500-dataset?region=us-east-1&tab=objects


Esta capa contiene datos exactamente como fueron recibidos, sin transformaciones.

---

## 2. Capa Bronze – Carga en RDS

Un segundo DAG de Airflow toma los archivos CSV desde S3 (carpeta `henry-sp500-dataset/`) y los carga en **Amazon RDS (PostgreSQL)**.  

Este nivel representa:

- Tablas que replican fielmente los datos crudos.
- Estructura SQL para facilitar transformaciones posteriores.
- Primer paso hacia la estandarización.

---

## 3. Capa Silver – Transformación Intermedia 

Un tercer DAG llevará los datos desde las tablas Bronze hacia:
(carpeta `henry-sp500-dataset/silver`) 

Esta capa incluirá:

- Datos limpios  
- Validaciones  
- Enriquecimientos básicos  
- Estándares consistentes entre todas las fuentes  

Es el nivel óptimo para ser consumido por herramientas externas o iniciado por dbt.

---

## 4. Capa Gold – Transformaciones (WIP)

Se crearan tablas que permiten responder preguntas como:

- Evolución del S&P 500  
- Impacto de commodities  
- Growth de empresas  
- Volatilidad en períodos de crisis  

---

## 5. Salida / Consumo

Las tablas Gold serán:

- Insumos para dashboards  
- Material para análisis exploratorio  
- Bases para modelos predictivos a futuro  

---

## Anexo — 1. Diagrama en formato Mermaid 

![Diagrama de Arquitectura](/assets/diagram.svg)


## Anexo — 2. Diagrama end to end propuesto en formato Mermaid 

![Diagrama de Arquitectura](/assets/diagram_e2e.svg)