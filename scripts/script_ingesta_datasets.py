"""
Script para:
1. Descargar múltiples datasets de Kaggle usando kagglehub
2. Guardarlos en carpetas separadas
3. Subir todos los archivos descargados a un bucket S3 de AWS

Requisitos previos:
python -m pip install pyspark kagglehub boto3

"""

from dotenv import load_dotenv
load_dotenv()

import os
import kagglehub
import boto3
from botocore.exceptions import NoCredentialsError

AWS_BUCKET_NAME = "henry-sp500-datasets"
S3_PREFIX = "datasets-kaggle/"
DATASETS = [
    "vaghefi/company-reviews",
    "andrewmvd/sp-500-stocks",
    "dixitdatascientist/s-and-p-500-esg-risk-analysis",
    "chickenrobot/historical-stocks-of-companies-of-the-sp-and-500"
]
BASE_DIR = "datasets_kaggle"
os.makedirs(BASE_DIR, exist_ok=True)

def upload_to_s3(local_file, bucket, s3_path):
    s3 = boto3.client("s3")

    try:
        s3.upload_file(local_file, bucket, s3_path)
        print(f"✔ Archivo subido a S3: s3://{bucket}/{s3_path}")
    except FileNotFoundError:
        print("❌ Archivo no encontrado:", local_file)
    except NoCredentialsError:
        print("❌ No se encontraron credenciales de AWS.")


for dataset in DATASETS:
    print("\n====================================")
    print(f"Descargando dataset: {dataset}")
    print("====================================")

    path = kagglehub.dataset_download(dataset)
    print("Archivos guardados en:", path)

    dataset_name = dataset.replace("/", "_")
    dataset_folder = os.path.join(BASE_DIR, dataset_name)
    os.makedirs(dataset_folder, exist_ok=True)

    for root, dirs, files in os.walk(path):
        for file in files:
            src = os.path.join(root, file)
            dst = os.path.join(dataset_folder, file)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            os.system(f"cp '{src}' '{dst}'")
            print(f"→ Copiado: {file}")

            s3_key = f"{S3_PREFIX}{dataset_name}/{file}"
            upload_to_s3(dst, AWS_BUCKET_NAME, s3_key)

print("\n✔ PROCESO COMPLETADO")