"""Detecta sismos de Chile con ShakeMap e invoca la Lambda "shakemap-descargar" por cada uno.
Luego muestra el control que escribió "shakemap-validar" (activada sola por S3) y copia
los archivos del bucket a datos/raw/usgs_shakemap/ para tenerlos también en el computador.

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
from config import CHILE, RAW  # noqa: E402
from desplegar import BUCKET, cliente  # noqa: E402

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def sismos_con_shakemap(desde: str, minmag: float) -> list[str]:
    r = requests.get(URL, params={"format": "geojson", "starttime": desde, "minmagnitude": minmag,
                                  "producttype": "shakemap", "orderby": "magnitude", **CHILE}, timeout=60)
    r.raise_for_status()
    return [f["id"] for f in r.json()["features"]]


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
        clave_control = f"usgs_shakemap/evento={eid}/_control.json"
        for _ in range(60):  # esperar a que la Lambda validar (disparada por S3) termine
            try:
                control = json.loads(s3.get_object(Bucket=BUCKET, Key=clave_control)["Body"].read())
                break
            except s3.exceptions.NoSuchKey:
                time.sleep(3)
        else:
            print("La Lambda validar no escribió el control a tiempo; revisa: docker logs sismo-localstack")
            continue
        print(f"    Control (shakemap-validar): estado={control['estado']} "
              f"grillas={list(control['grillas'])} problemas={control['problemas']}")
        for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=f"usgs_shakemap/evento={eid}/").get("Contents", []):
            destino = RAW / obj["Key"]
            destino.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(BUCKET, obj["Key"], str(destino))
        print(f"    Copiado a {RAW / 'usgs_shakemap' / f'evento={eid}'}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--desde", default="2026-01-01")
    ap.add_argument("--minmag", type=float, default=6.0)
    ap.add_argument("--max", type=int, default=2)
    a = ap.parse_args()
    main(a.desde, a.minmag, a.max)
