"""Control de calidad del catálogo USGS (lo ejecuta Airflow después de cada extracción).

Revisa cada mes cargado y escribe un reporte en datos/_control/usgs_catalogo/.
Si más del 5 % de los registros es inválido, la tarea falla y Airflow la marca en rojo.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import CHILE, DATOS, SILVER, ahora_utc, log, ruta  # noqa: E402

FUENTE = "usgs_catalogo"
UMBRAL_INVALIDOS = 0.05
OBLIGATORIAS = ["event_id", "tiempo_utc", "latitud", "longitud", "magnitud"]


def validar_mes(anio: int, mes: int) -> dict:
    archivo = SILVER / FUENTE / f"anio={anio}" / f"mes={mes:02d}" / "eventos.parquet"
    if not archivo.exists():
        reporte = {"anio": anio, "mes": mes, "registros": 0, "estado": "sin datos"}
    else:
        df = pd.read_parquet(archivo)
        reglas = {
            "campos_obligatorios_vacios": df[OBLIGATORIAS].isna().any(axis=1),
            "latitud_fuera_de_chile": ~df["latitud"].between(CHILE["minlatitude"], CHILE["maxlatitude"]),
            "longitud_fuera_de_chile": ~df["longitud"].between(CHILE["minlongitude"], CHILE["maxlongitude"]),
            "profundidad_fuera_de_rango": ~df["profundidad_km"].between(-5, 700),
            "magnitud_fuera_de_rango": ~df["magnitud"].between(0, 10),
            "event_id_duplicado": df["event_id"].duplicated(keep="first"),
        }
        invalido = pd.concat(reglas, axis=1).any(axis=1)
        reporte = {
            "anio": anio, "mes": mes,
            "registros": int(len(df)),
            "invalidos": int(invalido.sum()),
            "detalle": {k: int(v.sum()) for k, v in reglas.items()},
            "magnitud_min": float(df["magnitud"].min()) if len(df) else None,
            "magnitud_max": float(df["magnitud"].max()) if len(df) else None,
        }
        proporcion = reporte["invalidos"] / max(len(df), 1)
        reporte["estado"] = "ok" if proporcion <= UMBRAL_INVALIDOS else "rechazado"
    reporte["revisado_utc"] = ahora_utc().isoformat()
    destino = ruta(DATOS, "_control", FUENTE, f"anio={anio}", f"mes={mes:02d}", "control.json")
    destino.write_text(json.dumps(reporte, indent=2, ensure_ascii=False))
    log(FUENTE, f"Control {anio}-{mes:02d}: {reporte}")
    if reporte["estado"] == "rechazado":
        raise ValueError(f"{reporte['invalidos']} registros inválidos de {reporte['registros']}")
    return reporte


if __name__ == "__main__":
    validar_mes(int(sys.argv[1]), int(sys.argv[2]))
