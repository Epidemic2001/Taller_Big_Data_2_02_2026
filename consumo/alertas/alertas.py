"""Consumo 1 - Alertamiento: avisa por Slack, Telegram y correo cuando ocurre un sismo relevante.

Lee de MongoDB (colección "emsc_eventos", que llena el consumidor de Kafka con el streaming de EMSC)
y avisa cuando un sismo cumple las dos reglas:
  1. magnitud >= umbral                      (por defecto 4,5)
  2. epicentro a <= radio km de una ciudad   (por defecto 300 km; usa el índice geoespacial 2dsphere)

Cada sismo se avisa una sola vez. Si EMSC revisa la magnitud en 0,5 o más, se avisa una
ACTUALIZACIÓN. Lo ya avisado queda en la colección "alertas_enviadas", así que reiniciar el
bot no repite alertas.

Canales (todos opcionales; la terminal siempre muestra la alerta):
  Correo    -> Mailpit (Docker, bandeja en http://localhost:8025), activo por defecto
  Slack     -> variable SLACK_WEBHOOK_URL
  Telegram  -> variables TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID
Se pueden definir en el entorno o en consumo/alertas/.env (ver .env.example).

Uso:
    python consumo/alertas/alertas.py --probar                 # envía UNA alerta de prueba con el último sismo guardado
    python consumo/alertas/alertas.py                          # revisa una vez y termina
    python consumo/alertas/alertas.py --continuo               # revisa cada 10 s (Ctrl+C para salir)
    python consumo/alertas/alertas.py --umbral 4 --radio 500   # reglas más sensibles
    python consumo/alertas/alertas.py --demo                   # DEMO EN VIVO: avisa cada sismo NUEVO que llegue (todo el mundo)
    python consumo/alertas/alertas.py --ciudades Santiago Calama
"""
from __future__ import annotations

import argparse
import math
import os
import smtplib
import sys
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import log  # noqa: E402

try:  # que la terminal de Windows no se caiga con tildes o símbolos
    sys.stdout.reconfigure(errors="replace")
except Exception:  # pragma: no cover
    pass

FUENTE = "alertas"
MONGO_URI = "mongodb://admin:sismologia@localhost:27017/"
BASE = "sismologia"
RADIO_TIERRA_KM = 6378.1

# Ciudades vigiladas (lat, lon)
CIUDADES = {
    "Arica": (-18.478, -70.321), "Iquique": (-20.214, -70.152), "Calama": (-22.456, -68.929),
    "Antofagasta": (-23.650, -70.400), "Copiapó": (-27.366, -70.332), "La Serena": (-29.903, -71.250),
    "Valparaíso": (-33.047, -71.613), "Santiago": (-33.449, -70.669), "Concepción": (-36.827, -73.050),
    "Temuco": (-38.739, -72.599), "Puerto Montt": (-41.472, -72.937), "Punta Arenas": (-53.163, -70.917),
}


# --------------------------------------------------------------------------- utilidades
def cargar_env() -> None:
    """Lee consumo/alertas/.env (CLAVE=valor) sin pisar variables ya definidas en el entorno."""
    archivo = Path(__file__).with_name(".env")
    if not archivo.exists():
        return
    for linea in archivo.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def distancia_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia entre dos puntos sobre la esfera (fórmula de haversine)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * RADIO_TIERRA_KM * math.asin(math.sqrt(a))


def ciudades_cercanas(lat: float, lon: float, ciudades: dict) -> list[tuple[str, float]]:
    """Todas las ciudades vigiladas ordenadas de la más cercana a la más lejana."""
    return sorted(((n, distancia_km(lat, lon, *xy)) for n, xy in ciudades.items()), key=lambda t: t[1])


def hora_chile(t: datetime) -> str:
    try:
        from zoneinfo import ZoneInfo
        return t.astimezone(ZoneInfo("America/Santiago")).strftime("%d-%m-%Y %H:%M:%S (hora de Chile)")
    except Exception:  # sin base de datos de zonas horarias (pip install tzdata)
        return t.astimezone(timezone.utc).strftime("%d-%m-%Y %H:%M:%S UTC")


def filtro_mongo(umbral: float, ciudades: dict, radio_km: float, desde: datetime) -> dict:
    """Consulta que usa el índice 2dsphere: epicentro dentro del círculo de cada ciudad."""
    filtro = {"magnitud": {"$gte": umbral}, "tiempo_utc": {"$gte": desde}}
    if radio_km < 10000:  # con 10.000 km o más se vigila todo el mundo: no hace falta filtrar por zona
        filtro["$or"] = [{"epicentro": {"$geoWithin": {"$centerSphere": [[lon, lat], radio_km / RADIO_TIERRA_KM]}}}
                         for lat, lon in ciudades.values()]
    return filtro


# --------------------------------------------------------------------------- mensaje
def armar_mensaje(ev: dict, cercanas: list[tuple[str, float]], tipo: str) -> dict:
    mag = ev.get("magnitud") or 0
    ciudad, km = cercanas[0]
    nivel, icono = ("MAYOR", "🔴") if mag >= 6 else ("FUERTE", "🟠") if mag >= 5 else ("MODERADO", "🟡")
    etiqueta = {"nueva": "", "actualizada": "ACTUALIZACIÓN · ", "prueba": "PRUEBA · "}[tipo]
    lat, lon = ev["latitud"], ev["longitud"]
    mapa = f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=7/{lat}/{lon}"
    otras = ", ".join(f"{n} {k:.0f} km" for n, k in cercanas[1:4])
    lineas = [
        f"{etiqueta}Sismo {nivel} M{mag:.1f} a {km:.0f} km de {ciudad}",
        f"Zona: {ev.get('region') or 'sin región'}",
        f"Hora: {hora_chile(ev['tiempo_utc'])}",
        f"Profundidad: {ev.get('profundidad_km')} km · Magnitud {ev.get('tipo_magnitud') or ''}",
        f"También cerca de: {otras}",
        f"Epicentro: {lat:.2f}, {lon:.2f} · {mapa}",
        f"Fuente: EMSC (id {ev['_id']})",
    ]
    return {
        "asunto": f"[Alerta sísmica] {etiqueta}M{mag:.1f} a {km:.0f} km de {ciudad}",
        "texto": "\n".join(lineas),
        "slack": f"{icono} *{lineas[0]}*\n" + "\n".join(lineas[1:]),
        "html": f"<h2>{icono} {lineas[0]}</h2><p>" + "<br>".join(lineas[1:]).replace(mapa, f'<a href="{mapa}">ver mapa</a>') + "</p>",
    }


# --------------------------------------------------------------------------- canales
def enviar_slack(m: dict) -> bool | None:
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return None
    r = requests.post(url, json={"text": m["slack"]}, timeout=10)
    r.raise_for_status()
    return True


def enviar_telegram(m: dict) -> bool | None:
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not (token and chat):
        return None
    r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      data={"chat_id": chat, "text": m["texto"], "disable_web_page_preview": "true"}, timeout=10)
    r.raise_for_status()
    return True


def enviar_correo(m: dict, host: str, puerto: int) -> bool:
    msg = EmailMessage()
    msg["Subject"], msg["From"] = m["asunto"], os.environ.get("ALERTA_CORREO_DE", "alertas@sismologia.local")
    msg["To"] = os.environ.get("ALERTA_CORREO_A", "equipo@sismologia.local")
    msg.set_content(m["texto"])
    msg.add_alternative(m["html"], subtype="html")
    with smtplib.SMTP(host, puerto, timeout=10) as s:
        if os.environ.get("SMTP_USUARIO"):
            s.starttls()
            s.login(os.environ["SMTP_USUARIO"], os.environ.get("SMTP_CLAVE", ""))
        s.send_message(msg)
    return True


def notificar(m: dict, args, estado: dict) -> list[str]:
    """Envía por cada canal configurado; un canal caído no impide los demás."""
    print("\n" + "=" * 70 + f"\n{m['texto']}\n" + "=" * 70, flush=True)
    enviados = ["terminal"]
    canales = [("Slack", enviar_slack), ("Telegram", enviar_telegram)]
    if not args.sin_correo and not estado.get("correo_caido"):
        canales.append(("correo", lambda x: enviar_correo(x, args.smtp_host, args.smtp_puerto)))
    for nombre, fn in canales:
        try:
            if fn(m):
                enviados.append(nombre)
        except (requests.RequestException, OSError, smtplib.SMTPException) as e:
            log(FUENTE, f"No se pudo enviar por {nombre} ({e.__class__.__name__}).")
            if nombre == "correo":
                estado["correo_caido"] = True
                log(FUENTE, "Correo desactivado en esta ejecución: ¿está Mailpit levantado? "
                            "(docker compose ... --profile alertas up -d)")
    log(FUENTE, "Alerta enviada por: " + ", ".join(enviados))
    return enviados


# --------------------------------------------------------------------------- lógica
def tipo_de_alerta(db, ev: dict) -> str | None:
    previa = db.alertas_enviadas.find_one({"_id": ev["_id"]})
    if previa is None:
        return "nueva"
    if abs((ev.get("magnitud") or 0) - (previa.get("magnitud") or 0)) >= 0.5:
        return "actualizada"
    return None


def revisar(db, args, ciudades: dict, estado: dict) -> int:
    desde = datetime.now(timezone.utc) - timedelta(hours=args.horas)
    filtro = filtro_mongo(args.umbral, ciudades, args.radio, desde)
    n = 0
    for ev in db.emsc_eventos.find(filtro).sort("tiempo_utc", 1):
        cercanas = ciudades_cercanas(ev["latitud"], ev["longitud"], ciudades)
        if cercanas[0][1] > args.radio:  # el círculo de Mongo es aproximado: se confirma con haversine
            continue
        tipo = tipo_de_alerta(db, ev)
        if not tipo:
            continue
        canales = notificar(armar_mensaje(ev, cercanas, tipo), args, estado)
        db.alertas_enviadas.replace_one({"_id": ev["_id"]}, {
            "_id": ev["_id"], "magnitud": ev.get("magnitud"), "ciudad": cercanas[0][0],
            "distancia_km": round(cercanas[0][1], 1), "tipo": tipo, "canales": canales,
            "enviada_utc": datetime.now(timezone.utc)}, upsert=True)
        n += 1
    return n


def marcar_previos(db, args, ciudades: dict) -> int:
    """Modo demo: los sismos que ya estaban guardados se dan por avisados, sin enviar nada."""
    filtro = filtro_mongo(args.umbral, ciudades, args.radio, datetime.now(timezone.utc) - timedelta(hours=args.horas))
    n = 0
    for ev in db.emsc_eventos.find(filtro):
        if db.alertas_enviadas.find_one({"_id": ev["_id"]}) is None:
            db.alertas_enviadas.insert_one({"_id": ev["_id"], "magnitud": ev.get("magnitud"), "tipo": "previo",
                                            "canales": [], "enviada_utc": datetime.now(timezone.utc)})
            n += 1
    return n


def probar(db, args, ciudades: dict, estado: dict) -> int:
    ev = db.emsc_eventos.find_one(sort=[("tiempo_utc", -1)])
    if not ev:
        log(FUENTE, "MongoDB no tiene sismos todavía. Corre el productor y el consumidor de Kafka (DEMO.md, sección 6).")
        return 0
    notificar(armar_mensaje(ev, ciudades_cercanas(ev["latitud"], ev["longitud"], ciudades), "prueba"), args, estado)
    return 1


def main() -> int:
    cargar_env()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mongo", default=MONGO_URI)
    ap.add_argument("--umbral", type=float, default=4.5, help="magnitud mínima (4,5)")
    ap.add_argument("--radio", type=float, default=300, help="radio en km alrededor de cada ciudad (300)")
    ap.add_argument("--ciudades", nargs="+", metavar="CIUDAD", help=f"ciudades a vigilar (todas: {', '.join(CIUDADES)})")
    ap.add_argument("--horas", type=float, default=24, help="solo sismos de las últimas N horas (24)")
    ap.add_argument("--continuo", action="store_true", help="seguir revisando")
    ap.add_argument("--intervalo", type=float, default=10, help="segundos entre revisiones (10)")
    ap.add_argument("--demo", action="store_true",
                    help="demo en vivo: sin filtros (M>=0, todo el mundo), revisa cada 2 s y no repite los sismos ya guardados")
    ap.add_argument("--probar", action="store_true", help="enviar una alerta de prueba con el último sismo guardado")
    ap.add_argument("--sin-correo", action="store_true", help="no enviar correo")
    ap.add_argument("--smtp-host", default=os.environ.get("SMTP_HOST", "localhost"))
    ap.add_argument("--smtp-puerto", type=int, default=int(os.environ.get("SMTP_PUERTO", "1025")))
    args = ap.parse_args()

    if args.demo:
        args.umbral, args.radio, args.intervalo, args.continuo = 0.0, 20000.0, 2.0, True
    ciudades = CIUDADES
    if args.ciudades:
        pedidas = {c.lower(): c for c in args.ciudades}
        ciudades = {n: xy for n, xy in CIUDADES.items() if n.lower() in pedidas}
        if not ciudades:
            log(FUENTE, f"Ninguna ciudad reconocida. Opciones: {', '.join(CIUDADES)}")
            return 1

    db = MongoClient(args.mongo, serverSelectionTimeoutMS=5000, tz_aware=True)[BASE]
    try:
        db.command("ping")
    except PyMongoError as e:
        log(FUENTE, f"No se pudo conectar a MongoDB ({e.__class__.__name__}). ¿Está levantado? docker compose --profile mongo up -d")
        return 1

    estado: dict = {}
    if args.probar:
        return 0 if probar(db, args, ciudades, estado) else 1

    canales = ["terminal"] + [c for c, ok in (("Slack", os.environ.get("SLACK_WEBHOOK_URL")),
                                              ("Telegram", os.environ.get("TELEGRAM_BOT_TOKEN"))) if ok]
    if not args.sin_correo:
        canales.append(f"correo ({args.smtp_host}:{args.smtp_puerto})")
    if args.demo:
        log(FUENTE, f"Demo: {marcar_previos(db, args, ciudades)} sismos ya guardados se dan por avisados. Esperando sismos nuevos...")
    log(FUENTE, f"Vigilando {len(ciudades)} ciudades: M >= {args.umbral} a <= {args.radio:.0f} km, "
                f"últimas {args.horas:g} h. Canales: {', '.join(canales)}")
    try:
        while True:
            try:
                n = revisar(db, args, ciudades, estado)
                if not n and not args.continuo:
                    log(FUENTE, "Sin sismos nuevos que cumplan las reglas.")
            except PyMongoError as e:
                log(FUENTE, f"MongoDB no respondió ({e.__class__.__name__}); se reintenta.")
            if not args.continuo:
                break
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        log(FUENTE, "Detenido.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
