"""Detecta sismos de Chile con ShakeMap e invoca la Lambda "shakemap-descargar" por cada uno.
Luego muestra el control que escribió "shakemap-validar" y el resumen de "shakemap-almacenar"
(las dos las activa S3 sola, en cadena), y copia los archivos de los buckets a datos/raw y
datos/silver (usgs_shakemap/) para tenerlos también en el computador.

    python herramientas/lambda/invocar.py --desde 2026-01-01 --minmag 6 --max 2

La primera invocación tarda 1-2 minutos: LocalStack descarga la imagen del runtime de Python.
"""
import argparse
import base64
import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import CHILE, RAW, SILVER  # noqa: E402
from desplegar import BUCKET, BUCKET_SILVER, cliente  # noqa: E402

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def sismos_con_shakemap(desde: str, minmag: float) -> list[str]:
    r = requests.get(URL, params={"format": "geojson", "starttime": desde, "minmagnitude": minmag,
                                  "producttype": "shakemap", "orderby": "magnitude", **CHILE}, timeout=60)
    r.raise_for_status()
    return [f["id"] for f in r.json()["features"]]


def esperar_json(s3, bucket: str, clave: str, intentos: int) -> dict | None:
    """Espera a que una Lambda disparada por S3 deje su archivo de resultado."""
    for _ in range(intentos):
        try:
            return json.loads(s3.get_object(Bucket=bucket, Key=clave)["Body"].read())
        except s3.exceptions.NoSuchKey:
            time.sleep(3)
    return None


def main(desde: str, minmag: float, maximo: int):
    lam, s3 = cliente("lambda"), cliente("s3")
    ids = sismos_con_shakemap(desde, minmag)[:maximo]
    print(f"Sismos con ShakeMap en Chile desde {desde} (M>={minmag}): {ids}\n")
    for eid in ids:
        print(f"--> Invocando Lambda shakemap-descargar para {eid} ...")
        r = lam.invoke(FunctionName="shakemap-descargar", Payload=json.dumps({"eventid": eid}),
                       LogType="Tail")
        print(base64.b64decode(r.get("LogResult", "")).decode("utf-8", "replace").strip())
        if r.get("FunctionError"):
            print("ERROR:", r["Payload"].read().decode())
            continue
        prefijo = f"usgs_shakemap/evento={eid}"
        control = esperar_json(s3, BUCKET, f"{prefijo}/_control.json", intentos=60)
        if control is None:
            print("La Lambda validar no escribió el control a tiempo; revisa: docker logs sismo-localstack")
            continue
        print(f"    Control (shakemap-validar): estado={control['estado']} "
              f"grillas={list(control['grillas'])} problemas={control['problemas']}")
        if control["estado"] == "ok":
            # la primera vez la Lambda almacenar tarda más: descomprime GDAL y psycopg
            resumen = esperar_json(s3, BUCKET_SILVER, f"{prefijo}/almacenado.json", intentos=100)
            if resumen is None:
                print("    La Lambda almacenar no terminó a tiempo; revisa: docker logs sismo-localstack")
            else:
                print(f"    Almacenado (shakemap-almacenar): COG {resumen['cog']} ({resumen['cog_bytes'] / 1e6:.1f} MB)"
                      f" | PostGIS: {resumen['teselas']} teselas, área por grado MMI (km²) "
                      f"{resumen['area_km2_por_mmi']}")
        for bucket, capa in ((BUCKET, RAW), (BUCKET_SILVER, SILVER)):
            for obj in s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefijo}/").get("Contents", []):
                destino = capa / obj["Key"]
                destino.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, obj["Key"], str(destino))
        print(f"    Copiado a {RAW / prefijo} y {SILVER / prefijo}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--desde", default="2026-01-01")
    ap.add_argument("--minmag", type=float, default=6.0)
    ap.add_argument("--max", type=int, default=2)
    a = ap.parse_args()
    main(a.desde, a.minmag, a.max)
