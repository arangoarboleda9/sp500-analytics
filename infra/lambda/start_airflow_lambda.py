import os
import time
import boto3

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")

INSTANCE_ID = os.environ["INSTANCE_ID"]
AIRFLOW_URL = os.environ["AIRFLOW_URL"]  # ej: http://X.X.X.X:8080

# Parámetro donde guardamos el timestamp del último acceso
LAST_ACCESS_PARAM = os.environ.get(
    "LAST_ACCESS_PARAM",
    "/sp500/airflow/last_access"
)


def lambda_handler(event, context):
    # 1) Registrar último acceso (epoch en segundos)
    now = int(time.time())
    ssm.put_parameter(
        Name=LAST_ACCESS_PARAM,
        Value=str(now),
        Type="String",
        Overwrite=True,
    )

    # 2) Obtener estado actual de la instancia
    response = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
    state = response["Reservations"][0]["Instances"][0]["State"]["Name"]

    if state in ["stopped", "stopping"]:
        # Encender la instancia
        ec2.start_instances(InstanceIds=[INSTANCE_ID])

        body = """
        <html>
          <head><title>Levantando Airflow...</title></head>
          <body style="font-family: Arial; text-align: center; margin-top: 50px;">
            <h1>Estamos levantando tu entorno de Airflow 🚀</h1>
            <p>La instancia EC2 se está iniciando.<br/>
               Probá recargar esta página en 2–3 minutos.</p>
          </body>
        </html>
        """

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/html"},
            "body": body,
        }

    # 3) Si ya está corriendo → redirect 302 a la URL real de Airflow
    return {
        "statusCode": 302,
        "headers": {"Location": AIRFLOW_URL},
        "body": "",
    }