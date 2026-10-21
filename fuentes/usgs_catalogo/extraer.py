"""Fuente 2 - USGS Catálogo (QuakeML / XML) - Ingesta batch orquestada con Apache Airflow.

Consulta la API FDSN Event del USGS mes a mes (para no superar el límite de
20.000 eventos por petición), guarda el QuakeML original en la capa cruda y lo
convierte a Parquet con ObsPy.

Uso directo (sin Airflow):
    python extraer.py --inicio 2026-07 --fin 2026-10 --minmag 2.5
En Airflow, el DAG de dag_airflow.py llama a extraer_mes() una vez por mes.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests
from obspy import read_events

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import CHILE, HTTP_HEADERS, RAW, SILVER, TIMEOUT, log, ruta  # noqa: E402

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
LIMITE_API = 20000
FUENTE = "usgs_catalogo"


def descargar_mes(anio: int, mes: int, minmag: float) -> Path | None:
    inicio = f"{anio:04d}-{mes:02d}-01"
    fin = f"{anio + (mes == 12):04d}-{mes % 12 + 1:02d}-01"
    params = {"format": "quakeml", "starttime": inicio, "endtime": fin,
              "minmagnitude": minmag, "orderby": "time-asc", **CHILE}
    r = requests.get(URL, params=params, headers=HTTP_HEADERS, timeout=TIMEOUT)
    if r.status_code == 204:
        log(FUENTE, f"{inicio}: sin eventos")
        return None
    r.raise_for_status()
    destino = ruta(RAW, FUENTE, f"anio={anio}", f"mes={mes:02d}", "eventos.quakeml.xml")
    destino.write_bytes(r.content)
    return destino


def quakeml_a_tabla(archivo: Path) -> pd.DataFrame:
    filas = []
    for ev in read_events(str(archivo), format="QUAKEML"):
        o = ev.preferred_origin() or (ev.origins[0] if ev.origins else None)
        m = ev.preferred_magnitude() or (ev.magnitudes[0] if ev.magnitudes else None)
        if o is None:
            continue
        q = o.quality
        filas.append({
            "event_id": str(ev.resource_id).split("eventid=")[-1].split("&")[0],
            "tiempo_utc": o.time.datetime,
            "latitud": o.latitude,
            "longitud": o.longitude,
            "profundidad_km": (o.depth or 0) / 1000,
            "incert_profundidad_km": (o.depth_errors.uncertainty or 0) / 1000 if o.depth_errors else None,
            "magnitud": m.mag if m else None,
            "tipo_magnitud": m.magnitude_type if m else None,
            "incert_magnitud": m.mag_errors.uncertainty if m and m.mag_errors else None,
            "lugar": ev.event_descriptions[0].text if ev.event_descriptions else None,
            "tipo_evento": ev.event_type,
            "modo_evaluacion": o.evaluation_mode,
            "n_estaciones": q.used_station_count if q else None,
            "gap_azimutal": q.azimuthal_gap if q else None,
            "error_estandar_s": q.standard_error if q else None,
            "agencia": o.creation_info.agency_id if o.creation_info else None,
        })
    return pd.DataFrame(filas)


def extraer_mes(anio: int, mes: int, minmag: float = 2.5) -> int:
    """Tarea idempotente de un mes: la llama Airflow en cada ejecución."""
    archivo = descargar_mes(anio, mes, minmag)
    if archivo is None:
        return 0
    df = quakeml_a_tabla(archivo)
    if len(df) >= LIMITE_API:
        log(FUENTE, f"AVISO {anio}-{mes:02d}: se alcanzó el límite de {LIMITE_API}; dividir la ventana")
    destino = ruta(SILVER, FUENTE, f"anio={anio}", f"mes={mes:02d}", "eventos.parquet")
    df.to_parquet(destino, index=False)
    log(FUENTE, f"{anio}-{mes:02d}: {len(df)} eventos -> {destino.relative_to(SILVER.parent)}")
    return len(df)


def meses(inicio: str, fin: str):
    a, m = map(int, inicio.split("-"))
    af, mf = map(int, fin.split("-"))
    while (a, m) <= (af, mf):
        yield a, m
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--inicio", default="2026-07", help="mes inicial AAAA-MM")
    ap.add_argument("--fin", default="2026-10", help="mes final AAAA-MM (incluido)")
    ap.add_argument("--minmag", type=float, default=2.5)
    a = ap.parse_args()
    total = sum(extraer_mes(an, me, a.minmag) for an, me in meses(a.inicio, a.fin))
    log(FUENTE, f"Total: {total} eventos")
