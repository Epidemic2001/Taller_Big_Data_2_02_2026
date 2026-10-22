"""Funciones AWS Lambda de la ingesta USGS ShakeMap (raster), encadenadas por eventos.

  descargar(event)  {"eventid": "us6000t04s"}
      Busca el ShakeMap del sismo en la API del USGS y sube a S3 (capa bronce):
        s3://sismo-bronze/usgs_shakemap/evento=<id>/evento.json   (metadatos del sismo)
        s3://sismo-bronze/usgs_shakemap/evento=<id>/raster.zip    (grillas ESRI originales)

  validar(event)    disparada sola por S3 (ObjectCreated) cuando llega un raster.zip
      Controla el archivo: que traiga las grillas MMI/PGA/PGV, que cada .flt tenga el
      tamaño que declara su .hdr y que la grilla caiga en una zona válida. Escribe el
      resultado en .../_control.json.

Solo usan la librería estándar de Python + boto3 (incluido en el runtime de Lambda).
La conversión de la grilla con GDAL a Parquet es parte del procesamiento (fase 4):
ver fuentes/usgs_shakemap/lambda_function.py.
"""
import hashlib
import io
import json
import os
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

BUCKET = os.environ.get("BUCKET", "sismo-bronze")
PREFIJO = "usgs_shakemap"
URL_DETALLE = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventid={}"
CAPAS = ["mmi_mean", "pga_mean", "pgv_mean"]


def _s3():
    # Dentro de LocalStack se usa su endpoint; en AWS real, el de siempre.
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint and os.environ.get("LOCALSTACK_HOSTNAME"):
        endpoint = f"http://{os.environ['LOCALSTACK_HOSTNAME']}:{os.environ.get('EDGE_PORT', '4566')}"
    return boto3.client("s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"}))


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "big-data-sismologia-usm/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def descargar(event, context=None):
    eid = event["eventid"]
    detalle = json.loads(_get(URL_DETALLE.format(eid)))
    p = detalle["properties"]
    lon, lat, profundidad = detalle["geometry"]["coordinates"]
    sm = p["products"]["shakemap"][0]
    raster = _get(sm["contents"]["download/raster.zip"]["url"])
    base = f"{PREFIJO}/evento={eid}"
    meta = {"eventid": eid, "magnitud": p.get("mag"), "lugar": p.get("place"),
            "latitud": lat, "longitud": lon, "profundidad_km": profundidad,
            "tiempo_utc": datetime.fromtimestamp(p["time"] / 1000, timezone.utc).isoformat(),
            "version_shakemap": sm.get("properties", {}).get("version"),
            "bytes_raster": len(raster), "sha256_raster": hashlib.sha256(raster).hexdigest(),
            "descargado_utc": datetime.now(timezone.utc).isoformat()}
    s3 = _s3()
    s3.put_object(Bucket=BUCKET, Key=f"{base}/evento.json",
                  Body=json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"))
    s3.put_object(Bucket=BUCKET, Key=f"{base}/raster.zip", Body=raster)  # -> dispara validar()
    print(f"[descargar] {eid} M{meta['magnitud']} {meta['lugar']}: "
          f"{len(raster) / 1e6:.1f} MB -> s3://{BUCKET}/{base}/raster.zip")
    return meta


def _leer_hdr(texto: str) -> dict:
    return {l.split()[0].upper(): l.split()[1] for l in texto.splitlines() if len(l.split()) >= 2}


def validar(event, context=None):
    resultados = []
    for rec in event["Records"]:
        bucket = rec["s3"]["bucket"]["name"]
        clave = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        s3 = _s3()
        contenido = s3.get_object(Bucket=bucket, Key=clave)["Body"].read()
        problemas, grillas = [], {}
        try:
            with zipfile.ZipFile(io.BytesIO(contenido)) as z:
                nombres = set(z.namelist())
                for capa in CAPAS:
                    if f"{capa}.flt" not in nombres or f"{capa}.hdr" not in nombres:
                        problemas.append(f"falta la grilla {capa}")
                        continue
                    hdr = _leer_hdr(z.read(f"{capa}.hdr").decode("ascii", "replace"))
                    filas, cols = int(hdr["NROWS"]), int(hdr["NCOLS"])
                    esperado = filas * cols * int(hdr.get("NBITS", 32)) // 8
                    real = z.getinfo(f"{capa}.flt").file_size
                    if real != esperado:
                        problemas.append(f"{capa}.flt mide {real} bytes, se esperaban {esperado}")
                    lon0, lat0 = float(hdr["ULXMAP"]), float(hdr["ULYMAP"])
                    if not (-180 <= lon0 <= 180 and -90 <= lat0 <= 90):
                        problemas.append(f"{capa}: coordenadas fuera de rango")
                    grillas[capa] = {"filas": filas, "columnas": cols, "celda_grados": float(hdr["XDIM"]),
                                     "esquina_lon": lon0, "esquina_lat": lat0, "nodata": hdr.get("NODATA")}
        except zipfile.BadZipFile:
            problemas.append("el archivo no es un zip válido")
        control = {"archivo": f"s3://{bucket}/{clave}", "bytes": len(contenido),
                   "sha256": hashlib.sha256(contenido).hexdigest(), "grillas": grillas,
                   "estado": "ok" if not problemas else "rechazado", "problemas": problemas,
                   "validado_utc": datetime.now(timezone.utc).isoformat()}
        destino = clave.rsplit("/", 1)[0] + "/_control.json"
        s3.put_object(Bucket=bucket, Key=destino, Body=json.dumps(control, indent=2).encode("utf-8"))
        print(f"[validar] {clave}: {control['estado']} {problemas or ''} -> s3://{bucket}/{destino}")
        resultados.append(control)
    return resultados
