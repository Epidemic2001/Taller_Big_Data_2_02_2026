"""Fuente 3 - CSN Chile, red C1 (miniSEED) - Ingesta de archivos masivos (Apache NiFi + ObsPy).

Descarga la señal sísmica continua desde el servidor FDSN oficial del CSN
(owl.csn.uchile.cl), en dos modos:
  masivo      -> un rango de fechas completo, en tramos de 1 hora por estación (backfill)
  incremental -> solo el tramo nuevo desde la última descarga (checkpoint por estación)

Por cada tramo:
  1) guarda el miniSEED original en la capa cruda;
  2) con ObsPy pasa de "cuentas" del instrumento a unidades físicas (m/s o m/s²)
     usando los metadatos StationXML de la estación;
  3) corta la señal en ventanas de 60 s y guarda una fila por ventana en Parquet.

Cita obligatoria de los datos: Universidad de Chile (2013). Red Sismologica Nacional.
International Federation of Digital Seismograph Networks. https://doi.org/10.7914/SN/C1

Uso:
    python extraer.py --modo masivo --inicio 2026-09-25T15:00 --fin 2026-09-25T17:00
    python extraer.py --modo incremental            # pensado para que NiFi lo ejecute cada 5 min
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from obspy import UTCDateTime, read_inventory
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNNoDataException

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import ESTADO, RAW, SILVER, log, ruta  # noqa: E402

BASE = "https://owl.csn.uchile.cl"
SERVICIOS = {"dataselect": f"{BASE}/fdsn95/dataselect/1", "station": f"{BASE}/fdsn95/station/1"}
RED = "C1"
FUENTE = "csn_chile"
VENTANA_S = 60
TRAMO_S = 3600
RETRASO_S = 300  # el archivo del CSN llega con unos minutos de retraso
ARCHIVO_ESTADO = ESTADO / "csn_incremental.json"

# Estaciones por defecto (zona central y norte). Canal HHZ = banda ancha vertical,
# HNZ = acelerógrafo vertical (movimiento fuerte).
ESTACIONES = ["BO01:HHZ", "BO02:HHZ", "AC01:HHZ", "AP01:HHZ", "VA05:HNZ", "B05O:HNZ"]


def cliente() -> Client:
    return Client(BASE, service_mappings=SERVICIOS, _discover_services=False, timeout=120)


def metadatos(c: Client, sta: str, cha: str):
    """StationXML con la respuesta del instrumento (se guarda una vez en la capa cruda)."""
    archivo = ruta(RAW, FUENTE, "metadatos", f"{RED}.{sta}.{cha}.xml")
    if archivo.exists():
        return read_inventory(str(archivo))
    inv = c.get_stations(network=RED, station=sta, channel=cha, level="response")
    inv.write(str(archivo), format="STATIONXML")
    return inv


def procesar_tramo(c: Client, sta: str, cha: str, t0: UTCDateTime, t1: UTCDateTime, inv) -> int:
    try:
        st = c.get_waveforms(RED, sta, "*", cha, t0, t1)
    except FDSNNoDataException:
        log(FUENTE, f"{sta}.{cha} {t0.strftime('%Y-%m-%d %H:%M')}: sin datos (vacío en el archivo)")
        return 0
    except Exception as e:  # timeout o error del servidor: se registra y se sigue con el próximo tramo
        log(FUENTE, f"{sta}.{cha} {t0.strftime('%Y-%m-%d %H:%M')}: ERROR {type(e).__name__}: {e}")
        return None
    dia = f"{t0.strftime('%Y-%m-%d')}"
    st.write(str(ruta(RAW, FUENTE, f"estacion={sta}", f"fecha={dia}",
                      f"{RED}.{sta}.{cha}.{t0.strftime('%Y%m%dT%H%M%S')}.mseed")), format="MSEED")

    st.merge(method=1, fill_value=None)
    st = st.split()                     # si hay vacíos, trabaja cada tramo continuo por separado
    st.detrend("demean")
    st.remove_sensitivity(inv)          # cuentas -> m/s (HH?) o m/s² (HN?)
    unidad = "m/s2" if cha[1] == "N" else "m/s"
    filas = []
    for tr in st:
        for w in tr.slide(window_length=VENTANA_S, step=VENTANA_S, include_partial_windows=True):
            x = w.data.astype("float32")
            if x.size == 0:
                continue
            filas.append({
                "red": RED, "estacion": sta, "ubicacion": w.stats.location, "canal": w.stats.channel,
                "inicio_utc": w.stats.starttime.datetime, "fin_utc": w.stats.endtime.datetime,
                "muestras_por_seg": float(w.stats.sampling_rate), "n_muestras": int(x.size),
                "unidad": unidad,
                "max_abs": float(np.abs(x).max()),
                "rms": float(np.sqrt(np.mean(x.astype("float64") ** 2))),
                "senal": x.tolist(),
            })
    if filas:
        destino = ruta(SILVER, FUENTE, f"fecha={dia}", f"estacion={sta}",
                       f"{cha}_{t0.strftime('%H%M%S')}.parquet")
        pd.DataFrame(filas).to_parquet(destino, index=False)
    log(FUENTE, f"{sta}.{cha} {t0.strftime('%Y-%m-%d %H:%M')}: {len(filas)} ventanas de {VENTANA_S}s")
    return len(filas)


def masivo(estaciones: list[str], inicio: UTCDateTime, fin: UTCDateTime) -> int:
    c, total = cliente(), 0
    for par in estaciones:
        sta, cha = par.split(":")
        inv = metadatos(c, sta, cha)
        t = inicio
        while t < fin:
            total += procesar_tramo(c, sta, cha, t, min(t + TRAMO_S, fin), inv) or 0
            t += TRAMO_S
    return total


def incremental(estaciones: list[str], primera_vez_horas: float) -> int:
    estado = json.loads(ARCHIVO_ESTADO.read_text()) if ARCHIVO_ESTADO.exists() else {}
    c, total = cliente(), 0
    fin = UTCDateTime() - RETRASO_S
    for par in estaciones:
        sta, cha = par.split(":")
        inv = metadatos(c, sta, cha)
        desde = UTCDateTime(estado[par]) if par in estado else fin - primera_vez_horas * 3600
        t = desde
        while t < fin:
            n = procesar_tramo(c, sta, cha, t, min(t + TRAMO_S, fin), inv)
            if n is None:  # error: el checkpoint queda aquí y el próximo ciclo reintenta este tramo
                break
            total += n
            t = min(t + TRAMO_S, fin)
        estado[par] = str(t)
    ruta(ARCHIVO_ESTADO).write_text(json.dumps(estado, indent=2))
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--modo", choices=["masivo", "incremental"], default="masivo")
    ap.add_argument("--inicio", default="2026-09-25T15:00:00", help="UTC, modo masivo")
    ap.add_argument("--fin", default="2026-09-25T17:00:00", help="UTC, modo masivo")
    ap.add_argument("--estaciones", default=",".join(ESTACIONES), help="lista ESTACION:CANAL separada por comas")
    ap.add_argument("--horas-iniciales", type=float, default=1.0,
                    help="modo incremental: cuántas horas bajar la primera vez")
    a = ap.parse_args()
    lista = a.estaciones.split(",")
    n = (masivo(lista, UTCDateTime(a.inicio), UTCDateTime(a.fin)) if a.modo == "masivo"
         else incremental(lista, a.horas_iniciales))
    log(FUENTE, f"Total: {n} ventanas guardadas")
