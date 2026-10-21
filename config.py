"""Configuración común del pipeline de ingesta - Big Data en Sismología.

Todas las fuentes escriben en dos capas del Data Lake local:
  datos/raw/     -> capa cruda (bronze): el archivo tal cual llega (XML, JSON, CSV, miniSEED, raster)
  datos/silver/  -> capa procesada: Parquet, particionado por fecha / evento / estación
"""
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DATOS = RAIZ / "datos"
RAW = DATOS / "raw"
LANDING = DATOS / "landing"   # zona de llegada que lee Spark (LastQuake)
SILVER = DATOS / "silver"
ESTADO = DATOS / "_estado"    # checkpoints de las cargas incrementales

# Rectángulo que cubre Chile continental y su costa
CHILE = {"minlatitude": -56.0, "maxlatitude": -17.0,
         "minlongitude": -76.0, "maxlongitude": -66.0}

HTTP_HEADERS = {"User-Agent": "big-data-sismologia-usm/1.0 (proyecto academico)"}
TIMEOUT = 120  # segundos por petición HTTP


def ruta(*partes) -> Path:
    """Devuelve una ruta de archivo creando sus carpetas si no existen."""
    p = Path(*partes)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def ahora_utc() -> datetime:
    return datetime.now(timezone.utc)


def log(fuente: str, mensaje: str) -> None:
    print(f"[{ahora_utc():%H:%M:%S}] [{fuente}] {mensaje}", flush=True)
