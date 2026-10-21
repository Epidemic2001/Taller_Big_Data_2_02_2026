"""Consumidor del tópico "sismos-emsc": almacena las alertas EMSC en MongoDB (base NoSQL).

Base "sismologia", dos colecciones:
  emsc_revisiones -> historial: un documento por mensaje (el create y cada update de EMSC)
  emsc_eventos    -> estado actual: un documento por sismo, siempre con su versión más reciente

Índices: (unid, ultima_actualizacion_utc desc) para el historial y 2dsphere sobre el epicentro.
Las escrituras son idempotentes: reprocesar el tópico o los .jsonl no duplica documentos.
Los offsets de Kafka se confirman solo después de escribir en MongoDB (no se pierden mensajes).

Uso:
    python consumidor_mongodb.py --kafka localhost:9092              # lo pendiente del tópico y termina
    python consumidor_mongodb.py --kafka localhost:9092 --continuo   # queda escuchando (Ctrl+C para salir)
    python consumidor_mongodb.py                                     # carga los .jsonl de la capa cruda
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from pymongo import ASCENDING, DESCENDING, GEOSPHERE, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import log  # noqa: E402
from consumidor_parquet import FUENTE, TOPICO, aplanar, leer_registros  # noqa: E402

MONGO_URI = "mongodb://admin:sismologia@localhost:27017/"
BASE = "sismologia"
CLAVE_DUPLICADA = 11000


def fecha(texto: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(texto)
    except (TypeError, ValueError):
        return None


def documento(registro: dict) -> dict | None:
    """Mensaje de EMSC -> documento de MongoDB (campos planos + GeoJSON + mensaje original)."""
    doc = aplanar(registro)
    if not doc["unid"]:
        return None
    msg = registro["mensaje"]
    p = msg["data"]["properties"]
    # misma versión del mismo sismo -> mismo _id, así un reproceso no la duplica
    doc["_id"] = f"{doc['unid']}|{p.get('lastupdate') or registro['recibido_utc']}|{doc['accion']}"
    for campo in ("tiempo_utc", "ultima_actualizacion_utc", "recibido_utc"):
        doc[campo] = fecha(doc[campo])
    lat, lon = doc["latitud"], doc["longitud"]
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)) and -90 <= lat <= 90 and -180 <= lon <= 180:
        doc["epicentro"] = {"type": "Point", "coordinates": [lon, lat]}  # GeoJSON: longitud primero
    doc["mensaje_original"] = msg
    return doc


def crear_indices(db) -> None:
    db.emsc_revisiones.create_index([("unid", ASCENDING), ("ultima_actualizacion_utc", DESCENDING)])
    db.emsc_revisiones.create_index([("epicentro", GEOSPHERE)])
    db.emsc_eventos.create_index([("epicentro", GEOSPHERE)])
    db.emsc_eventos.create_index([("tiempo_utc", DESCENDING)])
    db.emsc_eventos.create_index([("magnitud", DESCENDING)])


def guardar(db, registros: list[dict]) -> int:
    docs = [d for d in map(documento, registros) if d]
    if not docs:
        return 0
    db.emsc_revisiones.bulk_write(
        [UpdateOne({"_id": d["_id"]}, {"$setOnInsert": d}, upsert=True) for d in docs], ordered=False)

    # Estado actual: se reemplaza solo si la versión que llega no es más antigua que la guardada.
    # Si la guardada es más nueva, el filtro no coincide, el upsert choca con el _id existente
    # (error 11000) y ese mensaje atrasado se descarta.
    ops = []
    for d in docs:
        estado = {k: v for k, v in d.items() if k != "_id"}
        ops.append(UpdateOne(
            {"_id": d["unid"], "ultima_actualizacion_utc": {"$not": {"$gt": d["ultima_actualizacion_utc"]}}},
            {"$set": estado, "$setOnInsert": {"primera_recepcion_utc": d["recibido_utc"]}},
            upsert=True))
    try:
        db.emsc_eventos.bulk_write(ops, ordered=False)
    except BulkWriteError as e:
        if any(err["code"] != CLAVE_DUPLICADA for err in e.details["writeErrors"]):
            raise

    for d in docs:
        log(FUENTE, f"MongoDB <- {d['accion']} {d['unid']} M{d['magnitud']} {d['region']}")
    return len(docs)


def desde_kafka(db, servidor: str, continuo: bool) -> int:
    from kafka import KafkaConsumer
    c = KafkaConsumer(TOPICO, bootstrap_servers=servidor, group_id="mongodb-writer",
                      auto_offset_reset="earliest", enable_auto_commit=False)
    log(FUENTE, f"Leyendo el tópico '{TOPICO}' desde {servidor}" + (" (Ctrl+C para salir)" if continuo else ""))
    total, polls_vacios = 0, 0
    try:
        while continuo or polls_vacios < 10:  # sin --continuo termina tras ~10 s sin mensajes nuevos
            lote = c.poll(timeout_ms=1000, max_records=500)
            mensajes = [m for particion in lote.values() for m in particion]
            if not mensajes:
                polls_vacios += 1
                continue
            polls_vacios = 0
            total += guardar(db, [json.loads(m.value) for m in mensajes])
            c.commit()
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
    return total


def main(kafka: str | None, mongo: str, continuo: bool) -> int:
    db = MongoClient(mongo, serverSelectionTimeoutMS=5000, tz_aware=True)[BASE]
    try:
        db.command("ping")
    except PyMongoError as e:
        log(FUENTE, f"No se pudo conectar a MongoDB ({e.__class__.__name__}). "
                    "¿Está levantado? docker compose --profile kafka up -d")
        return 0
    crear_indices(db)
    n = desde_kafka(db, kafka, continuo) if kafka else guardar(db, leer_registros(None))
    log(FUENTE, f"Fin: {n} mensajes procesados -> {db.emsc_eventos.count_documents({})} eventos, "
                f"{db.emsc_revisiones.count_documents({})} revisiones en MongoDB '{BASE}'")
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kafka", help="bootstrap server, p.ej. localhost:9092 (sin esto lee la capa cruda)")
    ap.add_argument("--mongo", default=MONGO_URI, help=f"URI de MongoDB (por defecto {MONGO_URI})")
    ap.add_argument("--continuo", action="store_true", help="seguir escuchando el tópico")
    a = ap.parse_args()
    main(a.kafka, a.mongo, a.continuo)
