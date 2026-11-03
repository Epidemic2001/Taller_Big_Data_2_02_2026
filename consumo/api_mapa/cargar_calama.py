"""Carga en PostGIS la red vial y los sectores de Calama desde OpenStreetMap (API Overpass).

Son los datos de referencia que la API del mapa cruza con los ShakeMap:
  red_vial_calama   calles y caminos cortados en tramos entre intersecciones: el grafo sobre el que
                    se calculan las rutas de emergencia. Cada tramo guarda su tipo de vía y una
                    velocidad de referencia para estimar tiempos de viaje.
  sectores_calama   ciudades, pueblos, villas y poblaciones (etiqueta place de OSM). Cada uno recibe
                    como sector su polígono de Voronoi (la zona más cercana a él que a cualquier otro)
                    acotado por un radio según su tamaño. Así el mapa dice qué daño sufrió cada sector.

Cubre el mismo rectángulo que la infraestructura crítica (Calama, Chuquicamata, Chiu Chiu y las faenas).
La respuesta original de Overpass queda en datos/raw/osm_calama/ (capa cruda); con --sin-descargar
se vuelve a cargar desde ahí, sin internet.

Datos © colaboradores de OpenStreetMap, licencia ODbL (https://www.openstreetmap.org/copyright).

    python consumo/api_mapa/cargar_calama.py
    python consumo/api_mapa/cargar_calama.py --sin-descargar
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

import psycopg
import requests

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(RAIZ / "fuentes" / "usgs_shakemap"))
from config import HTTP_HEADERS, RAW, log, ruta  # noqa: E402
from infraestructura_calama import CAJA, OVERPASS, POSTGIS  # noqa: E402

FUENTE = "api_mapa"
POSTGIS = os.environ.get("POSTGIS_URL", POSTGIS)
ESQUEMA = RAIZ / "herramientas" / "postgis" / "esquema.sql"
CARPETA_RAW = RAW / "osm_calama"

# Velocidad de referencia (km/h) de cada tipo de vía, para estimar el tiempo de viaje de una ruta
VELOCIDADES = {
    "motorway": 100, "trunk": 80, "primary": 60, "secondary": 50, "tertiary": 40,
    "motorway_link": 50, "trunk_link": 40, "primary_link": 40, "secondary_link": 30, "tertiary_link": 30,
    "unclassified": 40, "road": 30, "residential": 30, "living_street": 10, "service": 20, "track": 20,
}
# Radio máximo (m) del sector de cada tipo de lugar: una ciudad abarca más que una población
RADIOS = {"city": 8000, "town": 5000, "village": 1500, "suburb": 2000, "quarter": 1200,
          "neighbourhood": 800, "hamlet": 600, "locality": 500, "isolated_dwelling": 300}

CONSULTAS = {
    "red_vial": f"""[out:json][timeout:180];
way["highway"~"^({'|'.join(VELOCIDADES)})$"]["area"!="yes"]["service"!~"^(parking_aisle|drive-through)$"]({CAJA});
out geom;""",
    "lugares": f"""[out:json][timeout:90];
nwr["place"~"^({'|'.join(RADIOS)})$"]["name"]({CAJA});
out center tags;""",
}


def descargar(nombre: str, sin_descargar: bool) -> list[dict]:
    """Respuesta de Overpass, guardada tal cual en datos/raw/osm_calama/<nombre>.json."""
    archivo = ruta(CARPETA_RAW, f"{nombre}.json")
    if not sin_descargar:
        for servidor in OVERPASS:
            try:
                r = requests.post(servidor, data={"data": CONSULTAS[nombre]}, headers=HTTP_HEADERS, timeout=240)
                r.raise_for_status()
                r.json()  # que sea JSON válido antes de guardarlo
                archivo.write_bytes(r.content)
                log(FUENTE, f"Overpass {nombre}: {len(r.content) / 1e6:.1f} MB -> {archivo}")
                break
            except (requests.RequestException, ValueError) as e:
                log(FUENTE, f"Overpass {servidor} no respondió ({e.__class__.__name__}); se prueba el siguiente")
        else:
            sys.exit("Ningún servidor Overpass respondió; intenta de nuevo en unos minutos.")
    elif not archivo.exists():
        sys.exit(f"No existe {archivo}: ejecuta primero sin --sin-descargar")
    return json.loads(archivo.read_text(encoding="utf-8"))["elements"]


def tramos(vias: list[dict]):
    """Corta cada vía de OSM en sus intersecciones: un tramo va de un cruce (o extremo) al siguiente.

    Un nodo es cruce si aparece en más de una vía (o dos veces en la misma, como en una rotonda)."""
    usos = Counter(n for v in vias for n in v["nodes"])
    for v in vias:
        nodos, puntos, etiquetas = v["nodes"], v["geometry"], v.get("tags", {})
        inicio = 0
        for i in range(1, len(nodos)):
            if i < len(nodos) - 1 and usos[nodos[i]] == 1:
                continue
            if nodos[inicio] != nodos[i]:
                wkt = "LINESTRING(" + ", ".join(f"{p['lon']} {p['lat']}" for p in puntos[inicio:i + 1]) + ")"
                yield (v["id"], nodos[inicio], nodos[i], etiquetas["highway"],
                       etiquetas.get("name") or etiquetas.get("ref"), VELOCIDADES[etiquetas["highway"]], wkt)
            inicio = i


def poblacion(etiquetas: dict) -> int | None:
    cifras = re.sub(r"\D", "", etiquetas.get("population", ""))
    return int(cifras) if cifras else None


def lugares(elementos: list[dict]) -> list[tuple]:
    filas, vistos = [], set()
    for e in elementos:
        punto = e.get("center") or e  # nodos traen lat/lon; vías y relaciones, su centro
        clave = (round(punto["lat"], 5), round(punto["lon"], 5))
        if clave in vistos:  # Voronoi necesita puntos distintos
            continue
        vistos.add(clave)
        et = e["tags"]
        filas.append((f"{e['type']}/{e['id']}", et["name"], et["place"], poblacion(et),
                      punto["lon"], punto["lat"], RADIOS[et["place"]]))
    return filas


def main(sin_descargar: bool) -> None:
    vias = [v for v in descargar("red_vial", sin_descargar) if v["type"] == "way" and len(v["nodes"]) > 1]
    sitios = lugares(descargar("lugares", sin_descargar))
    with psycopg.connect(POSTGIS) as con:
        con.execute(ESQUEMA.read_text(encoding="utf-8"))  # crea las tablas si la base es anterior a ellas

        con.execute("""CREATE TEMP TABLE tmp_tramos (osm_way_id bigint, nodo_a bigint, nodo_b bigint, tipo text,
                       nombre text, velocidad_kmh smallint, wkt text) ON COMMIT DROP""")
        with con.cursor().copy("COPY tmp_tramos FROM STDIN") as copia:
            for fila in tramos(vias):
                copia.write_row(fila)
        con.execute("TRUNCATE red_vial_calama RESTART IDENTITY")
        con.execute("""
            INSERT INTO red_vial_calama (osm_way_id, nodo_a, nodo_b, tipo, nombre, velocidad_kmh, longitud_m, geom)
            SELECT osm_way_id, nodo_a, nodo_b, tipo, nombre, velocidad_kmh, ST_Length(g), g
            FROM (SELECT *, ST_Transform(ST_GeomFromText(wkt, 4326), 5361) AS g FROM tmp_tramos) t""")

        con.execute("""CREATE TEMP TABLE tmp_lugares (osm_id text, nombre text, tipo text, poblacion integer,
                       lon float8, lat float8, radio real) ON COMMIT DROP""")
        con.cursor().executemany("INSERT INTO tmp_lugares VALUES (%s, %s, %s, %s, %s, %s, %s)", sitios)
        con.execute("DELETE FROM sectores_calama")
        # Voronoi: cada lugar se queda con la zona que está más cerca de él que de cualquier otro,
        # recortada a su radio. Con un solo lugar no hay Voronoi y el sector es el círculo.
        con.execute("""
            WITH l AS (SELECT *, ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 5361) AS geom
                       FROM tmp_lugares),
                 celdas AS (SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(geom), 0,
                                    ST_Expand(ST_Envelope(ST_Collect(geom)), 20000)))).geom AS celda FROM l)
            INSERT INTO sectores_calama (osm_id, nombre, tipo, poblacion, geom)
            SELECT l.osm_id, l.nombre, l.tipo, l.poblacion,
                   ST_Multi(ST_CollectionExtract(coalesce(ST_Intersection(c.celda, ST_Buffer(l.geom, l.radio, 16)),
                                                          ST_Buffer(l.geom, l.radio, 16)), 3))
            FROM l LEFT JOIN celdas c ON ST_Intersects(c.celda, l.geom)""")

        red = con.execute("""SELECT tipo, count(*), round(sum(longitud_m) / 1000) FROM red_vial_calama
                             GROUP BY tipo ORDER BY 3 DESC""").fetchall()
        sectores = con.execute("SELECT tipo, count(*) FROM sectores_calama GROUP BY tipo ORDER BY 2 DESC").fetchall()
    log(FUENTE, f"Red vial de Calama: {sum(n for _, n, _ in red):,} tramos, {sum(km for *_, km in red):,.0f} km "
                f"-> PostGIS red_vial_calama {{{', '.join(f'{t}: {km:.0f} km' for t, _, km in red)}}}")
    log(FUENTE, f"Sectores de Calama: {sum(n for _, n in sectores)} -> PostGIS sectores_calama {dict(sectores)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sin-descargar", action="store_true",
                    help="cargar desde datos/raw/osm_calama/ en vez de consultar Overpass")
    main(ap.parse_args().sin_descargar)
