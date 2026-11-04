"""Consumo 3 - API REST + mapa web: daños por sector y rutas de emergencia en Calama.

Lee directamente de la base geoespacial del ShakeMap (PostgreSQL + PostGIS) y del COG en S3, sin
copiar los datos:
  - qué intensidad (MMI) y daño potencial recibió cada sector de Calama y cada hospital,
    subestación o faena (raster del ShakeMap cruzado con ST_Clip y ST_Value);
  - los polígonos de intensidad (isosistas) para dibujarlos en el mapa;
  - rutas de emergencia sobre la red vial que evitan los tramos de mayor intensidad, incluida la
    ruta al hospital operativo más cercano.

    uvicorn app:app --port 8090          (desde consumo/api_mapa)
    python consumo/api_mapa/app.py       (lo mismo, desde la raíz del proyecto)

Mapa web: http://localhost:8090   ·   documentación interactiva de la API: http://localhost:8090/docs
"""
from __future__ import annotations

import os
import sys
import threading
from functools import lru_cache
from pathlib import Path
from typing import Annotated

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

CARPETA = Path(__file__).resolve().parent
RAIZ = CARPETA.parents[1]
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(CARPETA))
from config import SILVER  # noqa: E402
from escala import describir, leyenda  # noqa: E402
from rutas import PuntoFueraDeRed, RedVial, aviso_bloqueo  # noqa: E402

# Desde Windows PostGIS está en localhost:5433; dentro de Docker, el compose define postgis:5432
POSTGIS = os.environ.get("POSTGIS_URL", "postgresql://admin:sismologia@localhost:5433/sismologia")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:4566")  # LocalStack
CALAMA = "ST_Transform(ST_SetSRID(ST_MakePoint(-68.929, -22.456), 4326), 5361)"  # plaza de Calama
EVITAR_MMI = Annotated[float | None, Query(ge=2, le=10, description=(
    "Los tramos con MMI mayor o igual a este valor se bloquean (posible daño en la vía). "
    "Vacío: no se bloquea ninguno, solo se castigan según su intensidad."))]
PUNTO = Query(description="Latitud y longitud en grados, separadas por coma: -22.456,-68.929")

app = FastAPI(
    title="Sismos en Calama: daños y rutas de emergencia",
    description="Consumo de la base geoespacial del ShakeMap (PostgreSQL + PostGIS y S3). "
                "Intensidad por sector e instalación crítica, isosistas y rutas que evitan las zonas "
                "de mayor intensidad. Datos de red vial, sectores e infraestructura © colaboradores de "
                "OpenStreetMap (ODbL).",
    version="1.0")


def consultar(sql: str, parametros=None) -> list[tuple]:
    with psycopg.connect(POSTGIS) as con:
        return con.execute(sql, parametros).fetchall()


def feature(geojson: dict, propiedades: dict) -> dict:
    return {"type": "Feature", "geometry": geojson, "properties": propiedades}


def coleccion(features: list[dict], **extra) -> dict:
    return {"type": "FeatureCollection", "features": features, **extra}


def redondear(v, decimales: int):
    return None if v is None else round(float(v), decimales)


def punto(texto: str) -> tuple[float, float]:
    try:
        lat, lon = (float(p) for p in texto.split(","))
    except ValueError:
        raise HTTPException(422, f"'{texto}' no es un punto: usa latitud,longitud (por ejemplo -22.456,-68.929)") from None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise HTTPException(422, f"'{texto}' está fuera del rango de latitud y longitud")
    return lat, lon


def a_utm(lat: float, lon: float) -> tuple[float, float]:
    return consultar("SELECT ST_X(p), ST_Y(p) FROM ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 5361) p",
                     (lon, lat))[0]


# ------------------------------------------------------------------ sismos

SQL_EVENTOS = f"""
    SELECT e.event_id, e.magnitud, e.lugar, e.tiempo_utc, e.profundidad_km,
           ST_Y(ST_Transform(e.epicentro, 4326)), ST_X(ST_Transform(e.epicentro, 4326)),
           s.mmi_max, s.pga_max_g, s.version_shakemap, s.procesado_utc,
           round((ST_Distance(e.epicentro, {CALAMA}) / 1000)::numeric),
           (SELECT ST_Value(t.rast, 1, {CALAMA}) FROM shakemap_teselas t
            WHERE t.event_id = e.event_id AND ST_Intersects(t.rast, {CALAMA})
            ORDER BY ST_Value(t.rast, 1, {CALAMA}) DESC NULLS LAST LIMIT 1) AS mmi_calama
    FROM eventos e JOIN shakemaps s USING (event_id)"""


def evento_a_dict(f) -> dict:
    (eid, mag, lugar, tiempo, prof, lat, lon, mmi_max, pga_max, version, procesado, dist, mmi_calama) = f
    return {"event_id": eid, "magnitud": mag, "lugar": lugar,
            "tiempo_utc": tiempo.isoformat() if tiempo else None, "profundidad_km": prof,
            "epicentro": {"lat": redondear(lat, 4), "lon": redondear(lon, 4)},
            "distancia_calama_km": int(dist) if dist is not None else None,
            "mmi_max": redondear(mmi_max, 2), "pga_max_g": redondear(pga_max, 3),
            "mmi_calama": redondear(mmi_calama, 2), "intensidad_calama": describir(mmi_calama),
            "shakemap_cubre_calama": mmi_calama is not None,
            "version_shakemap": version, "procesado_utc": procesado.isoformat() if procesado else None}


@app.get("/api/eventos", tags=["sismos"], summary="Sismos con ShakeMap, ordenados por su intensidad en Calama")
def eventos():
    filas = consultar(SQL_EVENTOS + " ORDER BY mmi_calama DESC NULLS LAST, e.magnitud DESC")
    return [evento_a_dict(f) for f in filas]


@app.get("/api/eventos/{event_id}", tags=["sismos"], summary="Un sismo, su ShakeMap y la intensidad en Calama")
def evento(event_id: str):
    filas = consultar(SQL_EVENTOS + " WHERE e.event_id = %s", (event_id,))
    if not filas:
        raise HTTPException(404, f"No hay ShakeMap del sismo {event_id} en PostGIS: cárgalo con "
                                 "herramientas/lambda/invocar.py")
    return evento_a_dict(filas[0])


@app.get("/api/eventos/{event_id}/isosistas", tags=["sismos"],
         summary="Polígonos de intensidad MMI del ShakeMap (GeoJSON)")
def isosistas(event_id: str):
    evento(event_id)
    filas = consultar("""
        SELECT mmi, round(ST_Area(geom) / 1e6),
               ST_AsGeoJSON(ST_Transform(ST_SimplifyPreserveTopology(geom, 250), 4326), 5)::json
        FROM shakemap_isosistas WHERE event_id = %s ORDER BY mmi""", (event_id,))
    return coleccion([feature(g, {"mmi": mmi, "area_km2": float(area), **describir(mmi)})
                      for mmi, area, g in filas])


@app.get("/api/eventos/{event_id}/cog", tags=["sismos"],
         summary="Descarga el ShakeMap completo (Cloud Optimized GeoTIFF de 3 bandas: MMI, PGA, PGV) desde S3")
def cog(event_id: str):
    filas = consultar("SELECT cog_s3 FROM shakemaps WHERE event_id = %s", (event_id,))
    if not filas:
        raise HTTPException(404, f"No hay ShakeMap del sismo {event_id}")
    bucket, clave = filas[0][0].removeprefix("s3://").split("/", 1)
    nombre = f"shakemap_{event_id}_utm19s.tif"
    cabeceras = {"Content-Disposition": f'attachment; filename="{nombre}"'}
    try:
        import boto3
        from botocore.config import Config
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name="us-east-1", aws_access_key_id="test",
                          aws_secret_access_key="test",
                          config=Config(s3={"addressing_style": "path"}, connect_timeout=3, retries={"max_attempts": 1}))
        datos = s3.get_object(Bucket=bucket, Key=clave)["Body"].read()
        return Response(datos, media_type="image/tiff", headers={**cabeceras, "X-Origen": f"s3://{bucket}/{clave}"})
    except Exception:
        # LocalStack gratuito no guarda los buckets al apagarse: invocar.py deja una copia en datos/silver
        local = SILVER / clave
        if local.exists():
            return FileResponse(local, media_type="image/tiff", filename=nombre, headers={"X-Origen": str(local)})
        raise HTTPException(503, "El COG no está en S3 (¿LocalStack apagado?) ni en datos/silver: vuelve a "
                                 "ejecutar herramientas/lambda/invocar.py") from None


# ------------------------------------------------------------------ daños

def datos_sectores(event_id: str) -> list[tuple]:
    # ST_Clip: estadística de la MMI dentro de cada sector. Un sector más chico que una celda de 1 km
    # puede no contener ningún centro de celda: entonces se usa ST_Value en un punto del sector.
    return consultar("""
        SELECT s.osm_id, s.nombre, s.tipo, s.poblacion, round((ST_Area(s.geom) / 1e6)::numeric, 2),
               coalesce(x.mmi_prom, p.mmi), coalesce(x.mmi_max, p.mmi),
               ST_AsGeoJSON(ST_Transform(s.geom, 4326), 5)::json
        FROM sectores_calama s
        LEFT JOIN LATERAL (
            SELECT CASE WHEN (st).count > 0 THEN (st).mean END AS mmi_prom,
                   CASE WHEN (st).count > 0 THEN (st).max END AS mmi_max
            FROM (SELECT ST_SummaryStatsAgg(ST_Clip(t.rast, 1, s.geom, true), 1, true) AS st
                  FROM shakemap_teselas t
                  WHERE t.event_id = %(id)s AND ST_Intersects(t.rast, s.geom)) y) x ON true
        LEFT JOIN LATERAL (
            SELECT ST_Value(t.rast, 1, ST_PointOnSurface(s.geom)) AS mmi FROM shakemap_teselas t
            WHERE t.event_id = %(id)s AND ST_Intersects(t.rast, ST_PointOnSurface(s.geom))
            ORDER BY 1 DESC NULLS LAST LIMIT 1) p ON true
        ORDER BY coalesce(x.mmi_max, p.mmi) DESC NULLS LAST, s.nombre""", {"id": event_id})


@app.get("/api/eventos/{event_id}/sectores", tags=["daños"],
         summary="Sectores de Calama con la intensidad que recibieron y su daño potencial (GeoJSON)")
def sectores(event_id: str):
    evento(event_id)
    return coleccion([
        feature(g, {"osm_id": oid, "nombre": nombre, "tipo": tipo, "poblacion": pob, "area_km2": float(area),
                    "mmi_prom": redondear(prom, 2), "mmi_max": redondear(mx, 2), **describir(mx)})
        for oid, nombre, tipo, pob, area, prom, mx, g in datos_sectores(event_id)])


def datos_infraestructura(event_id: str) -> list[dict]:
    # ST_Value: intensidad, aceleración (PGA) y velocidad (PGV) del suelo en cada instalación
    filas = consultar("""
        SELECT i.osm_id, i.tipo, i.nombre, v.mmi, v.pga, v.pgv,
               round((ST_Distance(i.geom, e.epicentro) / 1000)::numeric),
               ST_Y(ST_Transform(i.geom, 4326)), ST_X(ST_Transform(i.geom, 4326)), ST_X(i.geom), ST_Y(i.geom)
        FROM infraestructura_calama i
        JOIN eventos e ON e.event_id = %s
        LEFT JOIN LATERAL (
            SELECT ST_Value(t.rast, 1, i.geom) AS mmi, ST_Value(t.rast, 2, i.geom) AS pga,
                   ST_Value(t.rast, 3, i.geom) AS pgv
            FROM shakemap_teselas t WHERE t.event_id = e.event_id AND ST_Intersects(t.rast, i.geom)
            ORDER BY 1 DESC NULLS LAST LIMIT 1) v ON true
        ORDER BY v.mmi DESC NULLS LAST, i.tipo, i.nombre""", (event_id,))
    return [{"osm_id": oid, "tipo": tipo, "nombre": nombre, "mmi": redondear(mmi, 2), "pga_g": redondear(pga, 3),
             "pgv_cm_s": redondear(pgv, 1), "distancia_epicentro_km": int(dist) if dist is not None else None,
             "lat": round(lat, 6), "lon": round(lon, 6), "x": x, "y": y, **describir(mmi)}
            for oid, tipo, nombre, mmi, pga, pgv, dist, lat, lon, x, y in filas]


@app.get("/api/eventos/{event_id}/infraestructura", tags=["daños"],
         summary="Hospitales, clínicas, subestaciones y faenas con la intensidad que recibieron (GeoJSON)")
def infraestructura(event_id: str):
    evento(event_id)
    return coleccion([feature({"type": "Point", "coordinates": [i["lon"], i["lat"]]},
                              {k: v for k, v in i.items() if k not in ("lat", "lon", "x", "y")})
                      for i in datos_infraestructura(event_id)])


@app.get("/api/eventos/{event_id}/resumen", tags=["daños"],
         summary="Resumen del impacto en Calama: sectores, instalaciones y red vial por nivel de daño")
def resumen(event_id: str, evitar_mmi: EVITAR_MMI = 7.0):
    ev = evento(event_id)
    por_dano: dict[str, dict] = {}
    for _, nombre, _, pob, _, _, mx, _ in datos_sectores(event_id):
        d = describir(mx)
        fila = por_dano.setdefault(d["dano"], {"dano": d["dano"], "nivel_dano": d["nivel_dano"],
                                               "color": d["color"], "sectores": 0, "poblacion": 0, "ejemplos": []})
        fila["sectores"] += 1
        fila["poblacion"] += pob or 0
        if len(fila["ejemplos"]) < 5:
            fila["ejemplos"].append(nombre)
    instalaciones: dict[tuple, int] = {}
    for i in datos_infraestructura(event_id):
        clave = (i["tipo"], i["dano"], i["nivel_dano"])
        instalaciones[clave] = instalaciones.get(clave, 0) + 1
    red = red_vial()
    vias = red.intensidad_por_grado(intensidad_tramos(event_id, ev["procesado_utc"], red.firma), evitar_mmi)
    clave_orden = lambda x: (x["nivel_dano"] is None, -(x["nivel_dano"] or 0))  # noqa: E731
    return {
        "evento": ev,
        "sectores_por_dano": sorted(por_dano.values(), key=clave_orden),
        "infraestructura_por_dano": sorted(
            [{"tipo": t, "dano": d, "nivel_dano": n, "instalaciones": c} for (t, d, n), c in instalaciones.items()],
            key=lambda x: (x["nivel_dano"] is None, -(x["nivel_dano"] or 0), x["tipo"])),
        "red_vial": {"km_total": round(vias["km_total"], 1),
                     "km_por_grado": {describir(g)["romano"]: round(k, 1) for g, k in vias["km_por_grado"].items()},
                     "evitar_mmi": evitar_mmi, "km_bloqueados": round(vias["km_bloqueados"], 1)},
    }


# ------------------------------------------------------------------ rutas de emergencia

_red: RedVial | None = None
_candado = threading.Lock()


def red_vial() -> RedVial:
    """La red vial se carga en memoria una vez y se recarga solo si cambia la tabla en PostGIS."""
    global _red
    firma = consultar("SELECT count(*), coalesce(max(id), 0), coalesce(sum(longitud_m), 0)::bigint "
                      "FROM red_vial_calama")[0]
    with _candado:
        if _red is None or _red.firma != firma:
            filas = consultar("""
                SELECT id, nodo_a, nodo_b, tipo, nombre, velocidad_kmh, longitud_m,
                       (ST_AsGeoJSON(ST_Transform(geom, 4326), 6)::json) -> 'coordinates',
                       ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)),
                       ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom))
                FROM red_vial_calama""")
            _red = RedVial(filas, firma)
    if not _red.tramos:
        raise HTTPException(503, "La red vial de Calama no está cargada: ejecuta consumo/api_mapa/cargar_calama.py")
    return _red


@lru_cache(maxsize=16)
def intensidad_tramos(event_id: str, procesado_utc: str | None, firma_red: tuple) -> dict[int, float]:
    """MMI máxima de cada tramo de la red vial en un sismo (ST_Value sobre el raster del ShakeMap).

    Se muestrea el centro de los tramos cortos y un punto cada 500 m en los largos (las celdas son de
    1 km). Queda en caché por sismo; si el ShakeMap se vuelve a procesar, cambia procesado_utc."""
    filas = consultar("""
        WITH muestras AS (
            SELECT r.id, (ST_Dump(CASE WHEN r.longitud_m <= 500 THEN ST_LineInterpolatePoint(r.geom, 0.5)
                                       ELSE ST_LineInterpolatePoints(r.geom, 500 / r.longitud_m, true) END)).geom AS p
            FROM red_vial_calama r)
        SELECT m.id, max(ST_Value(t.rast, 1, m.p))
        FROM muestras m JOIN shakemap_teselas t ON t.event_id = %s AND ST_Intersects(t.rast, m.p)
        GROUP BY m.id""", (event_id,))
    return {tid: float(mmi) for tid, mmi in filas if mmi is not None}


def preparar_ruta(event_id: str):
    ev = evento(event_id)
    red = red_vial()
    return ev, red, intensidad_tramos(event_id, ev["procesado_utc"], red.firma)


@app.get("/api/eventos/{event_id}/red-vial", tags=["rutas"],
         summary="Calles con daño potencial (MMI ≥ V) agrupadas por grado, y las que quedan bloqueadas (GeoJSON)")
def red_por_intensidad(event_id: str, evitar_mmi: EVITAR_MMI = 7.0):
    _, red, mmi = preparar_ruta(event_id)
    vias = red.intensidad_por_grado(mmi, evitar_mmi)
    features = [feature({"type": "MultiLineString", "coordinates": grupo["lineas"]},
                        {"bloqueado": bloqueado, "km": round(grupo["km"], 1), "tramos": len(grupo["lineas"]),
                         **describir(g)})
                for (g, bloqueado), grupo in vias["grupos"].items()]
    return coleccion(features, evitar_mmi=evitar_mmi)


@app.get("/api/eventos/{event_id}/ruta", tags=["rutas"],
         summary="Ruta segura (evita las zonas de mayor intensidad) y ruta directa entre dos puntos (GeoJSON)")
def ruta(event_id: str, desde: Annotated[str, PUNTO], hasta: Annotated[str, PUNTO], evitar_mmi: EVITAR_MMI = 7.0):
    _, red, mmi = preparar_ruta(event_id)
    try:
        origen = red.anclar(*a_utm(*punto(desde)), "origen")
        destino = red.anclar(*a_utm(*punto(hasta)), "destino")
        r = red.rutas(origen, destino, mmi, evitar_mmi)
    except PuntoFueraDeRed as e:
        raise HTTPException(422, str(e)) from None
    return coleccion([r["segura"], r["directa"]], evitar_mmi=evitar_mmi, aviso=r["aviso"],
                     rutas_coinciden=r["segura"]["geometry"] == r["directa"]["geometry"],
                     shakemap_cubre_ruta=bool(mmi))


@app.get("/api/eventos/{event_id}/ruta-hospital", tags=["rutas"],
         summary="Ruta segura al hospital o clínica operativo más cercano (GeoJSON)")
def ruta_hospital(event_id: str, desde: Annotated[str, PUNTO], evitar_mmi: EVITAR_MMI = 7.0):
    """Un hospital se considera operativo si la intensidad que recibió es menor que evitar_mmi."""
    _, red, mmi = preparar_ruta(event_id)
    centros = [i for i in datos_infraestructura(event_id) if i["tipo"] in ("hospital", "clínica")]
    if not centros:
        raise HTTPException(503, "No hay hospitales cargados: ejecuta fuentes/usgs_shakemap/infraestructura_calama.py")
    operativos = [c for c in centros if c["mmi"] is None or evitar_mmi is None or c["mmi"] < evitar_mmi]
    descartados = [c for c in centros if c not in operativos]
    destinos: dict[int, list[dict]] = {}
    for c in operativos:
        nodo, distancia = red.nodo_cercano(c["x"], c["y"])
        if distancia <= 1000:
            destinos.setdefault(nodo, []).append(c)
    if not destinos:
        raise HTTPException(422, f"Ningún hospital o clínica con MMI menor que {evitar_mmi} está junto a la red vial")
    try:
        origen = red.anclar(*a_utm(*punto(desde)), "origen")
        camino, mejor, alcanzables = red.mejor_destino(origen, destinos, mmi, evitar_mmi)
    except PuntoFueraDeRed as e:
        raise HTTPException(422, str(e)) from None
    if len(camino) < 2:
        raise HTTPException(422, f"El origen ya está junto a {mejor['nombre'] or 'un centro de salud'}")
    recorrido = red.recorrido(camino, mmi, evitar_mmi, "segura")

    def ficha(c):
        return {k: c[k] for k in ("osm_id", "tipo", "nombre", "mmi", "romano", "dano", "color", "lat", "lon")}
    recorrido["properties"]["destino"] = ficha(mejor)
    return coleccion([recorrido], evitar_mmi=evitar_mmi, aviso=aviso_bloqueo(recorrido, evitar_mmi),
                     alternativas=[{**ficha(c), "tiempo_min": c["tiempo_min"]} for c in alcanzables[1:6]],
                     descartados=[ficha(c) for c in descartados])


# ------------------------------------------------------------------ escala, estado y mapa web

@app.get("/api/escala", tags=["referencia"], summary="Escala MMI: percepción, daño potencial y color de cada grado")
def escala():
    return leyenda()


@app.get("/api/salud", tags=["referencia"], summary="Estado de la conexión a PostGIS y de los datos cargados")
def salud():
    try:
        conteos = consultar("""SELECT (SELECT count(*) FROM shakemaps), (SELECT count(*) FROM sectores_calama),
                                      (SELECT count(*) FROM infraestructura_calama),
                                      (SELECT count(*) FROM red_vial_calama)""")[0]
    except psycopg.Error as e:
        raise HTTPException(503, f"PostGIS no responde en {POSTGIS.split('@')[-1]}: {e.__class__.__name__}") from None
    return dict(zip(("shakemaps", "sectores", "infraestructura", "tramos_red_vial"), conteos))


app.mount("/static", StaticFiles(directory=CARPETA / "static"), name="static")


@app.get("/", include_in_schema=False)
def mapa():
    return FileResponse(CARPETA / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PUERTO", 8090)))
