"""Conexiones de los notebooks del consumo 5 a los almacenamientos del ecosistema.

Los notebooks leen directo de donde quedaron los datos, sin copiarlos:
  Lakehouse (catálogo USGS)  -> catálogo REST de Apache Iceberg + MinIO (bucket "lakehouse")
  Data Lake (trazas del CSN) -> MinIO del data lake (buckets "raw" y "curated")
  Modelo de ML (consumo 4)   -> sus versiones en datos/modelos/ y, si está levantado, su API

Las direcciones salen de variables de entorno: en Docker (servicio jupyterlab del compose) apuntan
a los nombres de los servicios; si no están definidas quedan en localhost, así que los notebooks
también se pueden abrir desde Windows con el .venv.
"""
from __future__ import annotations

import json
import os
import warnings
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree

import duckdb
import numpy as np
import obstore
import pandas as pd
import zarr
from obstore.store import S3Store
from pyiceberg.catalog import load_catalog

RAIZ = Path(__file__).resolve().parents[3]          # carpeta del proyecto (/opt/sismo en Docker)
MODELOS = RAIZ / "datos" / "modelos" / "intensidad_ems98"
INFORMES = RAIZ / "datos" / "informes"

LAKEHOUSE_S3 = os.environ.get("LAKEHOUSE_S3_ENDPOINT", "http://localhost:9000")
LAKEHOUSE_CATALOGO = os.environ.get("LAKEHOUSE_CATALOGO_URI", "http://localhost:8181")
DATALAKE_S3 = os.environ.get("DATALAKE_S3_ENDPOINT", "http://localhost:9002")
API_ML = os.environ.get("API_ML_URL", "http://localhost:8091")
USUARIO_S3, CLAVE_S3 = "admin", "sismologia2026"

SILVER_TABLA = "silver.usgs_sismos"
GOLD_TABLA = "gold.usgs_resumen_mensual"
FUENTE_CSN = "csn_chile"

# Macrozonas de Chile por latitud, de norte a sur (límites aproximados entre regiones)
MACROZONAS = [("Norte Grande", -17.0, -26.0), ("Norte Chico", -26.0, -32.0), ("Centro", -32.0, -36.0),
              ("Sur", -36.0, -44.0), ("Austral", -44.0, -56.0)]
# Ciudades de referencia para orientar los mapas (latitud, longitud)
CIUDADES = {"Arica": (-18.48, -70.31), "Calama": (-22.46, -68.93), "Antofagasta": (-23.65, -70.40),
            "Copiapó": (-27.37, -70.33), "La Serena": (-29.90, -71.25), "Santiago": (-33.45, -70.66),
            "Concepción": (-36.83, -73.05), "Puerto Montt": (-41.47, -72.94), "Punta Arenas": (-53.16, -70.91)}

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)
# DuckDB 1.5 todavía usa DataFrame.drop(inplace=True) al entregar resultados a pandas 3.1: es solo un aviso
warnings.filterwarnings("ignore", message="The inplace keyword in DataFrame.drop is deprecated")


# ------------------------------------------------------------------ Lakehouse (Iceberg)

def catalogo():
    """Catálogo REST de Iceberg: sabe qué tablas hay y cuál es su versión (snapshot) vigente."""
    return load_catalog("lakehouse", type="rest", uri=LAKEHOUSE_CATALOGO, **{
        "s3.endpoint": LAKEHOUSE_S3, "s3.access-key-id": USUARIO_S3,
        "s3.secret-access-key": CLAVE_S3, "s3.region": "us-east-1"})


def sql_macrozona(columna: str = "latitud") -> str:
    """Expresión SQL (DuckDB) que asigna la macrozona según la latitud."""
    casos = " ".join(f"WHEN {columna} <= {norte} AND {columna} > {sur} THEN '{nombre}'"
                     for nombre, norte, sur in MACROZONAS)
    return f"CASE {casos} ELSE 'Fuera de Chile' END"


def snapshots(tabla) -> pd.DataFrame:
    """Historial de versiones de una tabla Iceberg, con el mes y el origen de cada carga."""
    snaps = tabla.inspect.snapshots().to_pandas().sort_values("committed_at")
    for clave in ("mes", "origen"):
        snaps[clave] = snaps["summary"].map(lambda s: dict(s).get(clave))
    for clave in ("added-records", "deleted-records", "total-records"):
        snaps[clave] = pd.to_numeric(snaps["summary"].map(lambda s: dict(s).get(clave))).astype("Int64")
    return snaps[["committed_at", "snapshot_id", "operation", "mes", "origen",
                  "added-records", "deleted-records", "total-records"]].reset_index(drop=True)


# ------------------------------------------------------------------ Data Lake (MinIO)

def bucket(nombre: str, prefijo: str | None = None) -> S3Store:
    return S3Store(nombre, prefix=prefijo, endpoint=DATALAKE_S3, access_key_id=USUARIO_S3,
                   secret_access_key=CLAVE_S3, region="us-east-1", client_options={"allow_http": True})


def objetos(nombre: str, prefijo: str = f"{FUENTE_CSN}/") -> list[dict]:
    return [o for lote in obstore.list(bucket(nombre), prefix=prefijo) for o in lote]


def particion(ruta: str) -> dict:
    """'csn_chile/red=C1/estacion=AC01/...' -> {'red': 'C1', 'estacion': 'AC01', ...}"""
    return dict(parte.split("=", 1) for parte in ruta.split("/") if "=" in parte)


def tensores() -> list[str]:
    """Prefijos de las particiones curadas que ya tienen su tensor Zarr de ventanas."""
    return sorted(o["path"].removesuffix("/ventanas.zarr/zarr.json") for o in objetos("curated")
                  if o["path"].endswith("/ventanas.zarr/zarr.json"))


def abrir_tensor(prefijo: str) -> zarr.Group:
    """Abre el tensor sin descargarlo: Zarr baja cada bloque recién cuando se lee."""
    tienda = zarr.storage.ObjectStore(bucket("curated", f"{prefijo}/ventanas.zarr"), read_only=True)
    return zarr.open_group(store=tienda, mode="r")


def estaciones_csn() -> pd.DataFrame:
    """Ubicación de cada estación, del StationXML que usó el curado (datos/raw/csn_chile/metadatos)."""
    ns = {"s": "http://www.fdsn.org/xml/station/1"}
    filas = []
    for xml in sorted((RAIZ / "datos" / "raw" / FUENTE_CSN / "metadatos").glob("*.xml")):
        estacion = ElementTree.parse(xml).getroot().find(".//s:Station", ns)
        filas.append({"estacion": estacion.get("code"), "sitio": estacion.findtext("s:Site/s:Name", "", ns),
                      "latitud": float(estacion.findtext("s:Latitude", namespaces=ns)),
                      "longitud": float(estacion.findtext("s:Longitude", namespaces=ns))})
    return pd.DataFrame(filas).drop_duplicates("estacion").set_index("estacion")


def distancia_km(lat1, lon1, lat2, lon2):
    """Distancia sobre la superficie de la Tierra (haversine), en km."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def duckdb_datalake() -> duckdb.DuckDBPyConnection:
    """DuckDB con las credenciales del MinIO del data lake: lee s3://raw/... y s3://curated/... directo."""
    url = urlparse(DATALAKE_S3)
    con = duckdb.connect()
    con.sql("INSTALL httpfs")   # la imagen de Docker ya la trae; desde Windows la descarga la primera vez
    con.sql("LOAD httpfs")
    con.sql(f"""CREATE SECRET datalake (TYPE s3, KEY_ID '{USUARIO_S3}', SECRET '{CLAVE_S3}',
                ENDPOINT '{url.netloc}', URL_STYLE 'path', USE_SSL {str(url.scheme == 'https').lower()},
                REGION 'us-east-1')""")
    return con


# ------------------------------------------------------------------ Modelo de ML (consumo 4)

def versiones_modelo() -> list[dict]:
    """Metadatos de cada versión del modelo de intensidad, de la más antigua a la más nueva."""
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(MODELOS.glob("v*.json"))]
