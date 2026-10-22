"""Carga en PostGIS la infraestructura crítica de Calama desde OpenStreetMap (API Overpass).

Hospitales y clínicas, subestaciones eléctricas y faenas mineras dentro de un rectángulo que cubre
Calama, Chuquicamata, Radomiro Tomic, Ministro Hales y El Abra. Se guardan como puntos en
SIRGAS-Chile / UTM 19S (EPSG:5361) en la tabla infraestructura_calama, que se cruza con los
ShakeMap para saber qué intensidad recibió cada instalación.

Datos © colaboradores de OpenStreetMap, licencia ODbL (https://www.openstreetmap.org/copyright).

    python infraestructura_calama.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import psycopg
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import HTTP_HEADERS, TIMEOUT, log  # noqa: E402

FUENTE = "usgs_shakemap"
POSTGIS = "postgresql://admin:sismologia@localhost:5433/sismologia"
# servidor principal y espejos públicos: se prueban en orden porque a veces están saturados
OVERPASS = ["https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://maps.mail.ru/osm/tools/overpass/api/interpreter"]
CAJA = "-22.75,-69.25,-21.85,-68.45"  # sur, oeste, norte, este
CONSULTA = f"""[out:json][timeout:90];
(
  nwr["amenity"~"^(hospital|clinic)$"]({CAJA});
  nwr["power"="substation"]({CAJA});
  nwr["landuse"="quarry"]({CAJA});
  nwr["industrial"="mine"]({CAJA});
  nwr["man_made"="mineshaft"]({CAJA});
);
out center tags;"""


def tipo(etiquetas: dict) -> str:
    if etiquetas.get("amenity") == "hospital":
        return "hospital"
    if etiquetas.get("amenity") == "clinic":
        return "clínica"
    if etiquetas.get("power") == "substation":
        return "subestación eléctrica"
    return "faena minera"


def consultar_osm() -> list[dict]:
    for servidor in OVERPASS:
        try:
            r = requests.post(servidor, data={"data": CONSULTA}, headers=HTTP_HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()["elements"]
        except (requests.RequestException, ValueError) as e:
            log(FUENTE, f"Overpass {servidor} no respondió ({e.__class__.__name__}); se prueba el siguiente")
    sys.exit("Ningún servidor Overpass respondió; intenta de nuevo en unos minutos.")


def main() -> None:
    filas = []
    for e in consultar_osm():
        punto = e.get("center") or e  # nodos traen lat/lon; vías y relaciones, su centro
        etiquetas = e.get("tags", {})
        filas.append((f"{e['type']}/{e['id']}", tipo(etiquetas),
                      etiquetas.get("name") or etiquetas.get("operator"), punto["lon"], punto["lat"]))
    with psycopg.connect(POSTGIS) as con:
        con.cursor().executemany("""
            INSERT INTO infraestructura_calama (osm_id, tipo, nombre, geom)
            VALUES (%s, %s, %s, ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 5361))
            ON CONFLICT (osm_id) DO UPDATE SET tipo = EXCLUDED.tipo, nombre = EXCLUDED.nombre,
                                               geom = EXCLUDED.geom""", filas)
        resumen = con.execute("""SELECT tipo, count(*) FROM infraestructura_calama
                                 GROUP BY tipo ORDER BY count(*) DESC""").fetchall()
    log(FUENTE, f"Infraestructura de Calama (OpenStreetMap): {len(filas)} instalaciones -> PostGIS {dict(resumen)}")


if __name__ == "__main__":
    main()
