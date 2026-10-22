"""Despliega en LocalStack (AWS emulado) la ingesta y el almacenamiento ShakeMap: buckets S3 + 3 Lambdas.

Ejecutar con el perfil lambda levantado (docker compose --profile lambda up -d):
    python herramientas/lambda/desplegar.py

Crea:
  - buckets S3 "sismo-bronze" (llegada), "sismo-silver" (COG procesado) y "sismo-lambda" (código)
  - Lambda "shakemap-descargar"  (se invoca con el id del sismo)
  - Lambda "shakemap-validar"    (la activa S3 cada vez que llega un raster.zip)
  - Lambda "shakemap-almacenar"  (la activa S3 cuando validar escribe _control.json): COG en S3 y
                                  teselas raster, isosistas y metadatos en PostgreSQL + PostGIS
  - notificaciones S3 ObjectCreated -> shakemap-validar y -> shakemap-almacenar

shakemap-almacenar necesita GDAL (rasterio) y psycopg. La primera vez se descargan con pip sus
versiones para el Linux de Lambda (~1 minuto); el paquete se sube por S3 porque supera los 50 MB.
"""
import io
import json
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import ESTADO  # noqa: E402

AWS = dict(endpoint_url="http://localhost:4566", region_name="us-east-1",
           aws_access_key_id="test", aws_secret_access_key="test")
BUCKET = "sismo-bronze"
BUCKET_SILVER = "sismo-silver"
BUCKET_CODIGO = "sismo-lambda"
CARPETA = Path(__file__).parent
LIBRERIAS = ESTADO / "lambda_almacenar"          # caché local de las librerías para Linux
# numpy viene con rasterio. typing_extensions se declara aparte: psycopg la necesita en Python 3.12,
# pero pip decide esas dependencias condicionales según el Python de este computador, no el de Lambda.
DEPENDENCIAS = ["rasterio", "psycopg[binary]", "typing_extensions"]
IMAGEN_LAMBDA = "public.ecr.aws/lambda/python:3.12"   # la misma imagen con que LocalStack ejecuta la Lambda
CACHE = " ".join(DEPENDENCIAS) + " +libexpat"          # si cambia, las librerías se vuelven a armar
# Las Lambdas corren en la red "sismologia" del docker-compose: ahí PostGIS es postgis:5432
POSTGIS = {"PGHOST": "postgis", "PGPORT": "5432", "PGUSER": "admin", "PGPASSWORD": "sismologia",
           "PGDATABASE": "sismologia"}
FUNCIONES = {
    "shakemap-descargar": {"handler": "lambda_ingesta.descargar", "memoria": 512, "librerias": False},
    "shakemap-validar": {"handler": "lambda_ingesta.validar", "memoria": 512, "librerias": False},
    "shakemap-almacenar": {"handler": "lambda_almacenamiento.almacenar", "memoria": 1024, "librerias": True},
}
# qué archivo, al llegar a sismo-bronze, activa cada Lambda
DISPARADORES = {"shakemap-validar": "raster.zip", "shakemap-almacenar": "_control.json"}


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


def librerias_linux() -> Path:
    listo = LIBRERIAS / ".listo"
    if not listo.exists() or listo.read_text() != CACHE:
        print("Descargando rasterio (GDAL) y psycopg para el Linux de Lambda (solo la primera vez)...")
        shutil.rmtree(LIBRERIAS, ignore_errors=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--target", str(LIBRERIAS),
                        "--platform", "manylinux2014_x86_64", "--platform", "manylinux_2_28_x86_64",
                        "--python-version", "3.12", "--implementation", "cp", "--only-binary=:all:",
                        *DEPENDENCIAS], check=True)
        # El GDAL de rasterio usa libexpat, que no viene en la imagen de Lambda (Amazon Linux 2023): se
        # copia desde esa misma imagen a lib/, carpeta que Lambda agrega a LD_LIBRARY_PATH.
        (LIBRERIAS / "lib").mkdir()
        subprocess.run(["docker", "run", "--rm", "--entrypoint", "sh", "-v", f"{LIBRERIAS / 'lib'}:/salida",
                        IMAGEN_LAMBDA, "-c", "dnf install -y expat >/dev/null && cp -L /usr/lib64/libexpat.so.1 /salida/"],
                       check=True)
        listo.write_text(CACHE)
    return LIBRERIAS


def paquete(con_librerias: bool) -> bytes:
    zip_en_memoria = io.BytesIO()
    with zipfile.ZipFile(zip_en_memoria, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(CARPETA / "lambda_ingesta.py", "lambda_ingesta.py")
        if con_librerias:
            z.write(CARPETA / "lambda_almacenamiento.py", "lambda_almacenamiento.py")
            for archivo in librerias_linux().rglob("*"):
                if archivo.is_file() and "__pycache__" not in archivo.parts and archivo.name != ".listo":
                    z.write(archivo, archivo.relative_to(LIBRERIAS).as_posix())
    return zip_en_memoria.getvalue()


def main():
    esperar_localstack()
    s3, lam, iam = cliente("s3"), cliente("lambda"), cliente("iam")

    for bucket in (BUCKET, BUCKET_SILVER, BUCKET_CODIGO):
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket s3://{bucket} creado")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"Bucket s3://{bucket} ya existía")

    confianza = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                                                        "Principal": {"Service": "lambda.amazonaws.com"}}]}
    try:
        rol = iam.create_role(RoleName="lambda-sismo", AssumeRolePolicyDocument=json.dumps(confianza))["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        rol = iam.get_role(RoleName="lambda-sismo")["Role"]["Arn"]

    arns = {}
    for nombre, f in FUNCIONES.items():
        codigo = paquete(f["librerias"])
        s3.put_object(Bucket=BUCKET_CODIGO, Key=f"{nombre}.zip", Body=codigo)
        try:
            lam.delete_function(FunctionName=nombre)
        except lam.exceptions.ResourceNotFoundException:
            pass
        arns[nombre] = lam.create_function(
            FunctionName=nombre, Runtime="python3.12", Role=rol, Handler=f["handler"],
            Code={"S3Bucket": BUCKET_CODIGO, "S3Key": f"{nombre}.zip"}, Timeout=300, MemorySize=f["memoria"],
            Environment={"Variables": {"BUCKET": BUCKET, "BUCKET_SILVER": BUCKET_SILVER, **POSTGIS}})["FunctionArn"]
        lam.get_waiter("function_active_v2").wait(FunctionName=nombre)
        print(f"Lambda {nombre} desplegada ({f['handler']}, paquete de {len(codigo) / 1e6:.1f} MB)")

    reglas = []
    for nombre, sufijo in DISPARADORES.items():
        lam.add_permission(FunctionName=nombre, StatementId=f"s3-{int(time.time())}",
                           Action="lambda:InvokeFunction", Principal="s3.amazonaws.com",
                           SourceArn=f"arn:aws:s3:::{BUCKET}")
        reglas.append({"LambdaFunctionArn": arns[nombre], "Events": ["s3:ObjectCreated:*"],
                       "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": sufijo}]}}})
        print(f"Disparador listo: S3 ObjectCreated (*{sufijo}) -> {nombre}")
    s3.put_bucket_notification_configuration(Bucket=BUCKET,
                                             NotificationConfiguration={"LambdaFunctionConfigurations": reglas})
    print("\nSiguiente paso: python herramientas/lambda/invocar.py")


if __name__ == "__main__":
    main()
