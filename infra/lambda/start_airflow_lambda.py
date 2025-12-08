import os
import time
import boto3
import urllib.request
import urllib.error

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")

INSTANCE_ID = os.environ["INSTANCE_ID"]
AIRFLOW_URL = os.environ["AIRFLOW_URL"]  # ej: http://X.X.X.X:8080

# Parámetro donde guardamos el timestamp del último acceso
LAST_ACCESS_PARAM = os.environ.get(
    "LAST_ACCESS_PARAM",
    "/sp500/airflow/last_access"
)

# HTML que se muestra mientras se levanta la infraestructura
WAIT_PAGE = """
<html>
  <head>
    <meta charset="UTF-8" />
    <title>Levantando entorno Airflow - TP Data Engineering</title>
    <!-- Auto-refresh cada 10 segundos para reintentar -->
    <meta http-equiv="refresh" content="10">
  </head>
  <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; text-align: center; margin-top: 60px; color: #1f2933;">
    <h1 style="font-size: 28px; margin-bottom: 16px;">
      🚀 Bienvenido al TP de Data Engineering (Henry)
    </h1>

    <h2 style="font-size: 20px; font-weight: 500; margin-bottom: 24px;">
      Estamos levantando el entorno de Airflow on-demand
    </h2>

    <p style="font-size: 15px; max-width: 520px; margin: 0 auto 16px auto; line-height: 1.6;">
      Para optimizar costos en AWS, nuestro servidor de orquestación (EC2 + Airflow)
      se enciende solo cuando alguien accede a esta URL.
    </p>

    <p style="font-size: 15px; max-width: 520px; margin: 0 auto 24px auto; line-height: 1.6;">
      En este momento la instancia EC2 está iniciando o cargando los contenedores
      de Airflow y Postgres. En unos instantes vas a poder ver la interfaz de Airflow
      y ejecutar los DAGs del proyecto.
    </p>

    <div style="margin: 32px auto;">
      <div style="
        border: 4px solid #e0e7ff;
        border-top-color: #4f46e5;
        border-radius: 999px;
        width: 48px;
        height: 48px;
        margin: 0 auto 12px auto;
        animation: spin 1s linear infinite;
      "></div>
      <div style="font-size: 14px; color: #6b7280;">
        Esta página se actualizará automáticamente cada 10 segundos hasta que Airflow esté listo.
      </div>
    </div>

    <p style="font-size: 13px; color: #9ca3af; margin-top: 40px;">
      Demo: Airflow on-demand con Lambda + API Gateway + EC2.
    </p>

    <style>
      @keyframes spin {
        from { transform: rotate(0deg); }
        to   { transform: rotate(360deg); }
      }
    </style>
  </body>
</html>
"""


def airflow_is_ready(url: str) -> bool:
    """
    Intenta hacer un GET rápido a la URL de Airflow.
    Si responde 2xx/3xx, asumimos que está listo.
    Si hay timeout, conexión rechazada, etc., devolvemos False.
    """
    try:
        req = urllib.request.Request(url, method="GET")
        # timeout cortito, no queremos que la Lambda se cuelgue
        with urllib.request.urlopen(req, timeout=2) as resp:
            return 200 <= resp.status < 400
    except Exception:
        return False


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

    # 3) Si está apagada o apagándose → la encendemos y mostramos pantalla de espera
    if state in ["stopped", "stopping"]:
        ec2.start_instances(InstanceIds=[INSTANCE_ID])

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/html"},
            "body": WAIT_PAGE,
        }

    # 4) Si está en pending → todavía está booteando, mostramos pantalla de espera
    if state == "pending":
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/html"},
            "body": WAIT_PAGE,
        }

    # 5) Si está running, chequeamos si Airflow ya responde
    if state == "running":
        if not airflow_is_ready(AIRFLOW_URL):
            # EC2 está arriba, pero Airflow o Postgres todavía se están levantando
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "text/html"},
                "body": WAIT_PAGE,
            }

        # Airflow responde OK → redirigimos
        return {
            "statusCode": 302,
            "headers": {"Location": AIRFLOW_URL},
            "body": "",
        }

    # 6) Cualquier otro estado raro → mostramos pantalla de espera genérica
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/html"},
        "body": WAIT_PAGE,
    }