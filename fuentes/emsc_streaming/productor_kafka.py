"""Fuente 1 - EMSC Streaming (JSON) - Ingesta en tiempo real con Apache Kafka.

Abre una conexión WebSocket persistente con EMSC. El servidor empuja cada sismo
nuevo o actualizado apenas se publica (sin polling). Cada mensaje JSON:
  1) se guarda tal cual en la capa cruda (un archivo .jsonl por día), y
  2) se publica en el tópico de Kafka "sismos-emsc" (clave = id del evento).

Uso:
    python productor_kafka.py --duracion 600                       # sin broker: solo capa cruda
    python productor_kafka.py --kafka localhost:9092 --duracion 0  # con Kafka, sin límite de tiempo
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path

import websocket  # paquete websocket-client

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import RAW, ahora_utc, log, ruta  # noqa: E402

URL_WS = "wss://www.seismicportal.eu/standing_order/websocket"
TOPICO = "sismos-emsc"
FUENTE = "emsc_streaming"


class Productor:
    def __init__(self, kafka: str | None):
        self.n = 0
        self.kafka = None
        if kafka:
            from kafka import KafkaProducer  # paquete kafka-python
            self.kafka = KafkaProducer(bootstrap_servers=kafka, acks="all",
                                       key_serializer=str.encode,
                                       value_serializer=str.encode)
            log(FUENTE, f"Conectado a Kafka {kafka}, tópico '{TOPICO}'")
        else:
            log(FUENTE, "Sin broker Kafka: los mensajes quedan solo en la capa cruda (.jsonl)")

    def on_open(self, ws):
        log(FUENTE, "WebSocket abierto, esperando sismos...")

    def on_message(self, ws, mensaje: str):
        recibido = ahora_utc()
        datos = json.loads(mensaje)
        props = datos.get("data", {}).get("properties", {})
        unid = props.get("unid", "sin-id")
        registro = json.dumps({"recibido_utc": recibido.isoformat(), "mensaje": datos}, ensure_ascii=False)
        with open(ruta(RAW, FUENTE, f"fecha={recibido:%Y-%m-%d}", "mensajes.jsonl"), "a", encoding="utf-8") as f:
            f.write(registro + "\n")
        if self.kafka:
            self.kafka.send(TOPICO, key=unid, value=registro)
        self.n += 1
        log(FUENTE, f"{datos.get('action')} {unid} M{props.get('mag')} {props.get('flynn_region')}")

    def on_error(self, ws, error):
        log(FUENTE, f"Error: {error}")


def main(kafka: str | None, duracion: int) -> int:
    p = Productor(kafka)
    ws = websocket.WebSocketApp(URL_WS, on_open=p.on_open, on_message=p.on_message, on_error=p.on_error)
    if duracion > 0:
        threading.Timer(duracion, ws.close).start()
    # reconnect: si se corta la conexión, vuelve a conectarse a los 5 s
    ws.run_forever(ping_interval=30, ping_timeout=10, reconnect=5 if duracion == 0 else 0)
    if p.kafka:
        p.kafka.flush()
    log(FUENTE, f"Fin: {p.n} mensajes recibidos")
    return p.n


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kafka", help="bootstrap server, p.ej. localhost:9092")
    ap.add_argument("--duracion", type=int, default=600, help="segundos escuchando (0 = indefinido)")
    a = ap.parse_args()
    main(a.kafka, a.duracion)
