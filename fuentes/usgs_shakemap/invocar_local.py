"""Simula en el computador lo que haría AWS: detecta sismos de Chile con ShakeMap
y, por cada uno, invoca la Lambda descargar() y luego convertir() (como lo haría
el evento ObjectCreated de S3).

Uso:
    python invocar_local.py --desde 2026-01-01 --minmag 6 --max 3
"""
import argparse
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import CHILE, HTTP_HEADERS, TIMEOUT, log  # noqa: E402
from lambda_function import FUENTE, convertir, descargar  # noqa: E402

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def sismos_con_shakemap(desde: str, minmag: float) -> list[str]:
    params = {"format": "geojson", "starttime": desde, "minmagnitude": minmag,
              "producttype": "shakemap", "orderby": "magnitude", **CHILE}
    r = requests.get(URL, params=params, headers=HTTP_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return [f["id"] for f in r.json()["features"]]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default="2026-01-01")
    ap.add_argument("--minmag", type=float, default=6.0)
    ap.add_argument("--max", type=int, default=3, help="máximo de sismos a procesar")
    a = ap.parse_args()
    ids = sismos_con_shakemap(a.desde, a.minmag)[: a.max]
    log(FUENTE, f"{len(ids)} sismos con ShakeMap: {', '.join(ids)}")
    for eid in ids:
        try:
            convertir(descargar({"eventid": eid}))
        except Exception as e:  # en AWS, Lambda reintentaría el evento
            log(FUENTE, f"{eid}: ERROR {type(e).__name__}: {e}")
