"""Consumidor del tópico "sismos-emsc": aplana el JSON y lo guarda en Parquet.

EMSC envía el mismo sismo varias veces (action=create y luego update con valores
corregidos). Aquí se conserva solo la última versión de cada evento.

Uso:
    python consumidor_parquet.py                          # lee los .jsonl de la capa cruda
    python consumidor_parquet.py --kafka localhost:9092   # lee directamente del tópico
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import RAW, SILVER, log, ruta  # noqa: E402

TOPICO = "sismos-emsc"
FUENTE = "emsc_streaming"


def leer_registros(kafka: str | None) -> list[dict]:
    if kafka:
        from kafka import KafkaConsumer
        c = KafkaConsumer(TOPICO, bootstrap_servers=kafka, auto_offset_reset="earliest",
                          group_id="parquet-writer", consumer_timeout_ms=10000,
                          value_deserializer=bytes.decode)
        return [json.loads(m.value) for m in c]
    registros = []
    for archivo in sorted((RAW / FUENTE).glob("fecha=*/mensajes.jsonl")):
        with open(archivo, encoding="utf-8") as f:
            registros += [json.loads(linea) for linea in f if linea.strip()]
    return registros


def aplanar(r: dict) -> dict:
    msg = r["mensaje"]
    p = msg["data"]["properties"]
    return {
        "unid": p.get("unid"),
        "accion": msg.get("action"),
        "tiempo_utc": p.get("time"),
        "latitud": p.get("lat"),
        "longitud": p.get("lon"),
        "profundidad_km": p.get("depth"),
        "magnitud": p.get("mag"),
        "tipo_magnitud": p.get("magtype"),
        "region": p.get("flynn_region"),
        "tipo_evento": p.get("evtype"),
        "agencia": p.get("auth"),
        "catalogo": p.get("source_catalog"),
        "ultima_actualizacion_utc": p.get("lastupdate"),
        "recibido_utc": r["recibido_utc"],
    }


def main(kafka: str | None) -> int:
    registros = leer_registros(kafka)
    if not registros:
        log(FUENTE, "No hay mensajes todavía (EMSC solo envía cuando ocurre un sismo).")
        return 0
    df = pd.DataFrame([aplanar(r) for r in registros])
    for col in ("tiempo_utc", "ultima_actualizacion_utc", "recibido_utc"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    n_msgs = len(df)
    df = df.sort_values("recibido_utc").drop_duplicates("unid", keep="last")
    df["fecha"] = df["tiempo_utc"].dt.strftime("%Y-%m-%d")
    for fecha, grupo in df.groupby("fecha"):
        destino = ruta(SILVER, FUENTE, f"fecha={fecha}", "eventos.parquet")
        if destino.exists():  # combinar con lo ya guardado ese día
            grupo = (pd.concat([pd.read_parquet(destino), grupo.drop(columns="fecha")])
                     .sort_values("recibido_utc").drop_duplicates("unid", keep="last"))
        grupo.drop(columns="fecha", errors="ignore").to_parquet(destino, index=False)
    log(FUENTE, f"{n_msgs} mensajes -> {len(df)} eventos únicos en {SILVER / FUENTE}")
    return len(df)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kafka", help="bootstrap server, p.ej. localhost:9092")
    main(ap.parse_args().kafka)
