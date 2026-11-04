"""Carga al Data Lake (zona raw) la señal del CSN de una ventana pasada, tal como la deja NiFi.

El flujo de NiFi (DEMO.md, secciones 2 y 8) trae cada 5 minutos solo los últimos minutos de señal.
Para tener en el data lake un sismo que ya ocurrió, este script hace la misma petición que NiFi al
servicio FDSN del CSN, en tramos de 5 minutos, y guarda cada miniSEED sin modificar en el bucket
"raw", con la misma ruta, nombre y metadatos:
    csn_chile/red=C1/estacion=.../canal=.../anio=AAAA/dia_juliano=DDD/C1.EST.CAN.AAAA-MM-DDTHHMMSS.mseed
Después, datalake.py lo cura (cabeceras.parquet y ventanas.zarr) como cualquier archivo de NiFi.

El CSN guarda la señal continua solo unas semanas: sirve para sismos recientes.

Uso (los sismos M4,6-4,9 del 26-10-2026 al norte de La Tirana, frente a Coquimbo y en La Ligua):
    python cargar_ventana.py --inicio 2026-10-26T19:30 --fin 2026-10-26T21:00
    python datalake.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import obstore
import requests
from obspy import UTCDateTime

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import HTTP_HEADERS, log  # noqa: E402
from datalake import bucket  # noqa: E402
from extraer import ESTACIONES, FUENTE, RED, SERVICIOS  # noqa: E402

TRAMO_S = 300   # NiFi pide tramos de 5 minutos


def cargar(estaciones: list[str], inicio: UTCDateTime, fin: UTCDateTime) -> int:
    raw, guardados = bucket("raw"), 0
    for par in estaciones:
        sta, cha = par.split(":")
        t0 = inicio
        while t0 < fin:
            t1 = min(t0 + TRAMO_S, fin)
            desde, hasta = t0.strftime("%Y-%m-%dT%H:%M:%S"), t1.strftime("%Y-%m-%dT%H:%M:%S")
            r = requests.get(f"{SERVICIOS['dataselect']}/query", headers=HTTP_HEADERS, timeout=60,
                             params={"net": RED, "sta": sta, "cha": cha, "starttime": desde, "endtime": hasta})
            if r.status_code == 200 and r.content:
                clave = (f"{FUENTE}/red={RED}/estacion={sta}/canal={cha}/anio={t0.year}/"
                         f"dia_juliano={t0.julday:03d}/{RED}.{sta}.{cha}.{desde.replace(':', '')}.mseed")
                obstore.put(raw, clave, r.content, attributes={
                    "Content-Type": "application/vnd.fdsn.mseed", "red": RED, "estacion": sta, "canal": cha,
                    "inicio-utc": desde, "fin-utc": hasta, "ingestado-por": "cargar_ventana.py"})
                guardados += 1
            else:
                log(FUENTE, f"{sta}.{cha} {desde}: sin datos (HTTP {r.status_code})")
            t0 = t1
        log(FUENTE, f"{sta}.{cha}: ventana {inicio} - {fin} en la zona raw del data lake")
    return guardados


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--inicio", required=True, help="UTC, AAAA-MM-DDTHH:MM")
    ap.add_argument("--fin", required=True, help="UTC, AAAA-MM-DDTHH:MM")
    ap.add_argument("--estaciones", default=",".join(ESTACIONES), help="lista ESTACION:CANAL separada por comas")
    a = ap.parse_args()
    n = cargar(a.estaciones.split(","), UTCDateTime(a.inicio), UTCDateTime(a.fin))
    log(FUENTE, f"Total: {n} miniSEED en s3://raw/{FUENTE}/. Para curarlos: python datalake.py")
