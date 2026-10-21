"""Fuente 5 - USGS ShakeMap (raster ESRI Grid) - Ingesta basada en eventos con AWS Lambda + GDAL.

Dos funciones Lambda encadenadas por eventos:
  descargar(event)  -> se activa cuando se detecta un sismo relevante; baja el raster.zip
                       del ShakeMap y lo deja en la capa cruda (S3 o carpeta local).
  convertir(event)  -> se activa cuando aparece ese archivo (evento "ObjectCreated" de S3);
                       lee las grillas con GDAL (vía rasterio) y escribe una tabla de puntos
                       latitud / longitud / intensidad en Parquet.

Unidades del raster (manual ShakeMap 4): MMI en unidades lineales; PGA en ln(g); PGV en ln(cm/s).

En AWS: definir BUCKET como variable de entorno. Sin BUCKET se usa la carpeta datos/ local,
lo que permite probar las mismas funciones en el computador (ver invocar_local.py).
"""
from __future__ import annotations

import io
import os
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import HTTP_HEADERS, RAW, SILVER, TIMEOUT, log, ruta  # noqa: E402

FUENTE = "usgs_shakemap"
URL_DETALLE = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventid={}"
BUCKET = os.environ.get("BUCKET")
# grilla -> (nombre de columna, función para pasar a unidades físicas)
CAPAS = {
    "mmi_mean": ("mmi", lambda v: v),
    "pga_mean": ("pga_g", np.exp),
    "pgv_mean": ("pgv_cm_s", np.exp),
}


def _guardar(clave: str, contenido: bytes) -> str:
    if BUCKET:
        import boto3
        boto3.client("s3").put_object(Bucket=BUCKET, Key=clave, Body=contenido)
        return f"s3://{BUCKET}/{clave}"
    destino = ruta(RAW.parent, clave)
    destino.write_bytes(contenido)
    return str(destino)


def _leer(ubicacion: str) -> bytes:
    if ubicacion.startswith("s3://"):
        import boto3
        bucket, clave = ubicacion[5:].split("/", 1)
        return boto3.client("s3").get_object(Bucket=bucket, Key=clave)["Body"].read()
    return Path(ubicacion).read_bytes()


def descargar(event: dict, context=None) -> dict:
    """event = {"eventid": "us7000ss82"}"""
    eid = event["eventid"]
    det = requests.get(URL_DETALLE.format(eid), headers=HTTP_HEADERS, timeout=TIMEOUT).json()
    sm = det["properties"]["products"]["shakemap"][0]
    raster = requests.get(sm["contents"]["download/raster.zip"]["url"], headers=HTTP_HEADERS, timeout=TIMEOUT)
    raster.raise_for_status()
    clave = f"raw/{FUENTE}/evento={eid}/raster.zip"
    ubicacion = _guardar(clave, raster.content)
    p = det["properties"]
    meta = {"eventid": eid, "magnitud": p.get("mag"), "lugar": p.get("place"),
            "tiempo_ms": p.get("time"), "version_shakemap": sm.get("properties", {}).get("version"),
            "ubicacion": ubicacion}
    log(FUENTE, f"{eid}: raster.zip {len(raster.content) / 1e6:.1f} MB -> {ubicacion}")
    return meta


def convertir(event: dict, context=None) -> dict:
    """event = salida de descargar(), o un evento S3 ObjectCreated."""
    if "Records" in event:  # disparado por S3
        rec = event["Records"][0]["s3"]
        event = {"ubicacion": f"s3://{rec['bucket']['name']}/{rec['object']['key']}",
                 "eventid": rec["object"]["key"].split("evento=")[1].split("/")[0]}
    eid = event["eventid"]
    contenido = _leer(event["ubicacion"])
    columnas = {}
    with zipfile.ZipFile(io.BytesIO(contenido)) as z:
        nombres = set(z.namelist())
        for capa, (col, a_fisico) in CAPAS.items():
            if f"{capa}.flt" in nombres:
                columnas[col] = _leer_grilla(z, capa, a_fisico)
    if not columnas:
        raise ValueError("El raster.zip no contiene grillas mmi/pga/pgv")
    ref = next(iter(columnas.values()))
    df = pd.DataFrame({"latitud": ref["lat"], "longitud": ref["lon"]})
    for col, g in columnas.items():
        df[col] = g["valor"]
    df = df.dropna(subset=list(columnas)).reset_index(drop=True)
    for k in ("magnitud", "lugar"):
        if k in event:
            df[k] = event[k]
    df.insert(0, "eventid", eid)
    destino = ruta(SILVER, FUENTE, f"evento={eid}", "grilla.parquet")
    df.to_parquet(destino, index=False)
    log(FUENTE, f"{eid}: {len(df):,} puntos de grilla -> {destino}")
    return {"eventid": eid, "puntos": len(df), "parquet": str(destino)}


def _leer_grilla(z: zipfile.ZipFile, capa: str, a_fisico) -> dict:
    """Lee una grilla ESRI (.flt + .hdr) con GDAL. GDAL necesita ambos archivos juntos."""
    import tempfile

    import rasterio  # envoltorio de Python sobre GDAL

    with tempfile.TemporaryDirectory() as tmp:
        for ext in (".flt", ".hdr"):
            Path(tmp, capa + ext).write_bytes(z.read(capa + ext))
        with rasterio.open(Path(tmp, capa + ".flt")) as ds:
            datos = ds.read(1, masked=True).astype("float64")
            filas, cols = np.indices(datos.shape)
            xs, ys = rasterio.transform.xy(ds.transform, filas.ravel(), cols.ravel())
    valores = a_fisico(datos.filled(np.nan).ravel())
    return {"lat": np.asarray(ys), "lon": np.asarray(xs), "valor": valores}
