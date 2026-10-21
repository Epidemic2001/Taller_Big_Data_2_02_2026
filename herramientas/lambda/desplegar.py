"""Despliega en LocalStack (AWS emulado) la ingesta ShakeMap: bucket S3 + 2 funciones Lambda + disparador.

Ejecutar con LocalStack levantado (docker compose --profile lambda up -d):
    python herramientas/lambda/desplegar.py

Crea:
  - bucket S3 "sismo-bronze"
  - Lambda "shakemap-descargar"  (se invoca con el id del sismo)
  - Lambda "shakemap-validar"    (la activa S3 cada vez que llega un raster.zip)
  - notificación S3 ObjectCreated -> shakemap-validar
"""
import io
import json
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

AWS = dict(endpoint_url="http://localhost:4566", region_name="us-east-1",
           aws_access_key_id="test", aws_secret_access_key="test")
BUCKET = "sismo-bronze"
CODIGO = Path(__file__).with_name("lambda_ingesta.py")
FUNCIONES = {"shakemap-descargar": "lambda_ingesta.descargar",
             "shakemap-validar": "lambda_ingesta.validar"}


def cliente(servicio):
    return boto3.client(servicio, config=Config(s3={"addressing_style": "path"}), **AWS)


def esperar_localstack():
    for i in range(30):
        try:
            cliente("s3").list_buckets()
            return
        except Exception:
            print(f"Esperando a LocalStack... ({i + 1}/30)")
            time.sleep(5)
    sys.exit("LocalStack no responde en http://localhost:4566")


def main():
    esperar_localstack()
    s3, lam, iam = cliente("s3"), cliente("lambda"), cliente("iam")

    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket s3://{BUCKET} creado")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket s3://{BUCKET} ya existía")

    confianza = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                                                        "Principal": {"Service": "lambda.amazonaws.com"}}]}
    try:
        rol = iam.create_role(RoleName="lambda-sismo", AssumeRolePolicyDocument=json.dumps(confianza))["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        rol = iam.get_role(RoleName="lambda-sismo")["Role"]["Arn"]

    paquete = io.BytesIO()
    with zipfile.ZipFile(paquete, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(CODIGO, CODIGO.name)

    arns = {}
    for nombre, handler in FUNCIONES.items():
        try:
            lam.delete_function(FunctionName=nombre)
        except lam.exceptions.ResourceNotFoundException:
            pass
        arns[nombre] = lam.create_function(
            FunctionName=nombre, Runtime="python3.12", Role=rol, Handler=handler,
            Code={"ZipFile": paquete.getvalue()}, Timeout=300, MemorySize=512,
            Environment={"Variables": {"BUCKET": BUCKET}})["FunctionArn"]
        lam.get_waiter("function_active_v2").wait(FunctionName=nombre)
        print(f"Lambda {nombre} desplegada ({handler})")

    lam.add_permission(FunctionName="shakemap-validar", StatementId=f"s3-{int(time.time())}",
                       Action="lambda:InvokeFunction", Principal="s3.amazonaws.com",
                       SourceArn=f"arn:aws:s3:::{BUCKET}")
    s3.put_bucket_notification_configuration(Bucket=BUCKET, NotificationConfiguration={
        "LambdaFunctionConfigurations": [{
            "LambdaFunctionArn": arns["shakemap-validar"],
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": "raster.zip"}]}},
        }]})
    print("Disparador listo: S3 ObjectCreated (*raster.zip) -> shakemap-validar")
    print("\nSiguiente paso: python herramientas/lambda/invocar.py")


if __name__ == "__main__":
    main()
