import os
import time
import boto3

ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")

INSTANCE_ID = os.environ["INSTANCE_ID"]
LAST_ACCESS_PARAM = os.environ.get(
    "LAST_ACCESS_PARAM",
    "/sp500/airflow/last_access"
)
IDLE_SECONDS = int(os.environ.get("IDLE_SECONDS", "120"))  # 10 min por defecto


def lambda_handler(event, context):
    # 1) Leer último acceso
    try:
        param = ssm.get_parameter(Name=LAST_ACCESS_PARAM)
        last_access = int(param["Parameter"]["Value"])
    except ssm.exceptions.ParameterNotFound:
        # Nunca se accedió → no apagues nada
        return {"status": "no-param"}

    now = int(time.time())
    idle = now - last_access

    # 2) Ver estado actual de la instancia
    resp = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
    state = resp["Reservations"][0]["Instances"][0]["State"]["Name"]

    if state == "running" and idle > IDLE_SECONDS:
        ec2.stop_instances(InstanceIds=[INSTANCE_ID])
        return {"status": "stopped", "idle_seconds": idle}

    return {"status": "no-action", "state": state, "idle_seconds": idle}