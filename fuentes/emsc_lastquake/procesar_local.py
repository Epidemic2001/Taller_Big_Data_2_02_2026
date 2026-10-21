"""Versión local (pandas) del job de Spark, para computadores sin Java/Spark.

Hace la misma transformación que job_spark.py, en micro-batches: procesa solo
los CSV de la zona de llegada que aún no se han procesado (lleva un registro en
datos/_estado/lastquake_procesados.json) y los agrega al Parquet silver.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import ESTADO, LANDING, SILVER, ahora_utc, log, ruta  # noqa: E402

FUENTE = "emsc_lastquake"
COLUMNAS = ["longitud", "latitud", "tiempo_txt", "canal", "intensidad_cruda", "intensidad_corregida"]
REGISTRO = ESTADO / "lastquake_procesados.json"


def main() -> int:
    hechos = set(json.loads(REGISTRO.read_text())) if REGISTRO.exists() else set()
    nuevos = [p for p in sorted((LANDING / "lastquake").glob("*.csv")) if p.name not in hechos]
    if not nuevos:
        log(FUENTE, "Micro-batch vacío: no hay archivos nuevos")
        return 0
    partes = []
    for p in nuevos:
        df = pd.read_csv(p, comment="#", header=None, names=COLUMNAS)
        df["unid"] = re.match(r"(\d{8}_\d{7})__", p.name).group(1)
        partes.append(df)
    lote = pd.concat(partes, ignore_index=True)
    lote["tiempo_utc"] = pd.to_datetime(lote.pop("tiempo_txt").str.replace(" UTC", ""), utc=True)
    lote["procesado_utc"] = ahora_utc()
    for unid, grupo in lote.groupby("unid"):
        destino = ruta(SILVER, FUENTE, f"unid={unid}", "testimonios.parquet")
        if destino.exists():  # un sismo puede recibir testimonios nuevos en ciclos posteriores
            grupo = pd.concat([pd.read_parquet(destino), grupo]).drop_duplicates(
                ["longitud", "latitud", "tiempo_utc", "canal"], keep="last")
        grupo.drop(columns="unid").to_parquet(destino, index=False)
    ruta(REGISTRO).write_text(json.dumps(sorted(hechos | {p.name for p in nuevos})))
    log(FUENTE, f"Micro-batch: {len(nuevos)} archivos, {len(lote)} testimonios, {lote['unid'].nunique()} sismos")
    return len(lote)


if __name__ == "__main__":
    main()
