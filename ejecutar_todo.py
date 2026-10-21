"""Ejecuta la extracción de las 5 fuentes y muestra un resumen final.

    python ejecutar_todo.py                      # todo, con valores por defecto
    python ejecutar_todo.py --solo csn usgs      # solo algunas fuentes
    python ejecutar_todo.py --minutos-emsc 15    # escuchar EMSC más tiempo
    python ejecutar_todo.py --spark              # LastQuake con Spark real (requiere Java)

EMSC Streaming corre en paralelo mientras se procesan las otras fuentes.
"""
import argparse
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

RAIZ = Path(__file__).resolve().parent
sys.path.insert(0, str(RAIZ))
from config import SILVER  # noqa: E402

PY = sys.executable
ENV = {**os.environ, "PYTHONIOENCODING": "utf-8"}
F = RAIZ / "fuentes"


def correr(nombre: str, *args: str) -> bool:
    print(f"\n=== {nombre} ===", flush=True)
    r = subprocess.run([PY, *args], cwd=RAIZ, env=ENV)
    if r.returncode != 0:
        print(f"!!! {nombre} terminó con error (código {r.returncode}); se continúa con las demás fuentes")
    return r.returncode == 0


def resumen():
    print("\n================ RESUMEN (capa silver, Parquet) ================")
    for fuente in ["emsc_streaming", "usgs_catalogo", "csn_chile", "emsc_lastquake", "usgs_shakemap"]:
        archivos = list((SILVER / fuente).rglob("*.parquet"))
        filas = sum(pq.ParquetFile(a).metadata.num_rows for a in archivos)
        print(f"{fuente:16s} {len(archivos):5d} archivos Parquet {filas:12,d} filas")
    print(f"\nDatos en: {SILVER.parent}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--solo", nargs="*", default=["emsc", "usgs", "csn", "lastquake", "shakemap"])
    ap.add_argument("--minutos-emsc", type=int, default=5)
    ap.add_argument("--spark", action="store_true")
    a = ap.parse_args()
    hoy = date.today()

    emsc = None
    if "emsc" in a.solo:
        print("=== EMSC Streaming (en segundo plano) ===", flush=True)
        emsc = subprocess.Popen([PY, str(F / "emsc_streaming/productor_kafka.py"),
                                 "--duracion", str(a.minutos_emsc * 60)], cwd=RAIZ, env=ENV)

    if "usgs" in a.solo:
        inicio = (hoy.replace(day=1) - timedelta(days=90)).strftime("%Y-%m")
        correr("USGS Catálogo (QuakeML)", str(F / "usgs_catalogo/extraer.py"),
               "--inicio", inicio, "--fin", hoy.strftime("%Y-%m"))
    if "csn" in a.solo:
        correr("CSN Chile - carga masiva (miniSEED)", str(F / "csn_chile/extraer.py"), "--modo", "masivo")
        correr("CSN Chile - carga incremental", str(F / "csn_chile/extraer.py"), "--modo", "incremental")
    if "lastquake" in a.solo:
        correr("EMSC LastQuake - sondeo (CSV)", str(F / "emsc_lastquake/sondeo.py"),
               "--desde", (hoy - timedelta(days=30)).isoformat(), "--ciclos", "1")
        if a.spark:
            correr("LastQuake - Spark Structured Streaming", str(F / "emsc_lastquake/job_spark.py"), "--una-vez")
        else:
            correr("LastQuake - micro-batch local", str(F / "emsc_lastquake/procesar_local.py"))
    if "shakemap" in a.solo:
        correr("USGS ShakeMap (raster)", str(F / "usgs_shakemap/invocar_local.py"),
               "--desde", "2026-01-01", "--minmag", "6", "--max", "2")

    if emsc is not None:
        print(f"\n=== Esperando que termine la escucha de EMSC ({a.minutos_emsc} min en total) ===", flush=True)
        emsc.wait()
        correr("EMSC Streaming - consumidor a Parquet", str(F / "emsc_streaming/consumidor_parquet.py"))

    resumen()
