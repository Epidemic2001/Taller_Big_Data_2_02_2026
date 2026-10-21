"""Fuente 4 - EMSC LastQuake (CSV) - Sondeo de la API REST de testimonios.

Cada ciclo:
  1) pide la lista de sismos en Chile con testimonios actualizados desde el último ciclo;
  2) descarga los testimonios de cada sismo (zip con CSV) y guarda el zip en la capa cruda;
  3) deja el CSV del sismo en la zona de llegada (datos/landing/lastquake/),
     que es la carpeta que Spark Structured Streaming lee en micro-batches.

Licencia de los datos: CC BY 4.0 (citar EMSC).

Uso:
    python sondeo.py --desde 2026-09-01 --ciclos 1            # una pasada
    python sondeo.py --ciclos 0 --intervalo 60                # sondeo continuo cada minuto
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
import zipfile
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import CHILE, ESTADO, HTTP_HEADERS, LANDING, RAW, TIMEOUT, ahora_utc, log, ruta  # noqa: E402

URL = "https://www.seismicportal.eu/testimonies-ws/api/search"
FUENTE = "emsc_lastquake"
ARCHIVO_ESTADO = ESTADO / "lastquake.json"
CAJA = {"minlat": CHILE["minlatitude"], "maxlat": CHILE["maxlatitude"],
        "minlon": CHILE["minlongitude"], "maxlon": CHILE["maxlongitude"]}


def listar_eventos(desde: str | None, actualizado_despues: str | None) -> list[str]:
    params = {**CAJA, "format": "text", "limit": 500}
    if actualizado_despues:
        params["updatedafter"] = actualizado_despues
    if desde:
        params["starttime"] = desde
    r = requests.get(URL, params=params, headers=HTTP_HEADERS, timeout=TIMEOUT)
    if r.status_code == 204:
        return []
    r.raise_for_status()
    lineas = [l for l in r.text.splitlines() if l and not l.startswith("#")]
    return [l.split("|")[0] for l in lineas]


def descargar_testimonios(unid: str, marca: str) -> int:
    params = {"unids": f"[{unid}]", "includeTestimonies": "true",
              "testimonyformat": "text", "includeDoc": "false"}
    r = requests.get(URL, params=params, headers=HTTP_HEADERS, timeout=TIMEOUT)
    if r.status_code == 204:
        return 0
    r.raise_for_status()
    ruta(RAW, FUENTE, f"evento={unid}", f"testimonios_{marca}.zip").write_bytes(r.content)
    n = 0
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        for nombre in z.namelist():
            if nombre == f"{unid}.csv":
                contenido = z.read(nombre)
                n = sum(1 for l in contenido.decode("utf-8").splitlines() if l and not l.startswith("#"))
                # el nombre del archivo lleva el id del sismo: Spark lo recupera con input_file_name()
                ruta(LANDING, "lastquake", f"{unid}__{marca}.csv").write_bytes(contenido)
            elif nombre == "events.csv":
                ruta(RAW, FUENTE, f"evento={unid}", "evento.csv").write_bytes(z.read(nombre))
    return n


def ciclo(desde: str | None) -> int:
    estado = json.loads(ARCHIVO_ESTADO.read_text()) if ARCHIVO_ESTADO.exists() else {}
    inicio = ahora_utc()
    marca = f"{inicio:%Y%m%dT%H%M%S}"
    unids = listar_eventos(desde, estado.get("ultimo_ciclo_utc"))
    total = 0
    for unid in unids:
        n = descargar_testimonios(unid, marca)
        total += n
        log(FUENTE, f"{unid}: {n} testimonios")
        time.sleep(0.5)  # ser amables con el servidor
    ruta(ARCHIVO_ESTADO).write_text(json.dumps({"ultimo_ciclo_utc": inicio.strftime("%Y-%m-%dT%H:%M:%S")}))
    log(FUENTE, f"Ciclo listo: {len(unids)} sismos, {total} testimonios en {LANDING / 'lastquake'}")
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--desde", default=None, help="fecha inicial AAAA-MM-DD (primera carga)")
    ap.add_argument("--ciclos", type=int, default=1, help="cantidad de ciclos (0 = infinito)")
    ap.add_argument("--intervalo", type=int, default=60, help="segundos entre ciclos")
    a = ap.parse_args()
    i = 0
    while True:
        ciclo(a.desde if i == 0 else None)
        i += 1
        if a.ciclos and i >= a.ciclos:
            break
        time.sleep(a.intervalo)
