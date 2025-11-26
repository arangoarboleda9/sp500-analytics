# Imagen base ligera de Python
FROM python:3.11-slim

# Evitar que Python genere .pyc y ayudar al logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Carpeta de trabajo dentro del contenedor
WORKDIR /app

# Copiamos requirements primero para aprovechar cache de Docker
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código de ETL
COPY etl/ ./etl/

# Comando por defecto: ejecutar el ETL
# Podés cambiar "etl/main.py" por tu script real
CMD ["python", "etl/main.py"]
