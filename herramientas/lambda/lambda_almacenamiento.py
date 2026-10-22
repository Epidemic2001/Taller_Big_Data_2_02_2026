"""Función AWS Lambda que almacena el ShakeMap: archivo pesado en S3 y datos geoespaciales en PostGIS.

  almacenar(event)  disparada sola por S3 (ObjectCreated) cuando shakemap-validar escribe _control.json
      Si el control está "ok":
        1. lee las grillas MMI, PGA y PGV del raster.zip con GDAL (rasterio) y las reproyecta a
           SIRGAS-Chile 2002 / UTM 19S (EPSG:5361) en celdas de 1 km;
        2. sube un Cloud Optimized GeoTIFF (COG) de 3 bandas a S3, para mapas web:
             s3://sismo-silver/usgs_shakemap/evento=<id>/shakemap_utm19s.tif
        3. en PostgreSQL + PostGIS, en una transacción que reemplaza lo que hubiera del sismo:
             eventos             el sismo, con su epicentro como geometría
             shakemaps           metadatos y ubicación del COG en S3
             shakemap_teselas    el raster en teselas de 100 x 100 (ST_FromGDALRaster + ST_Tile)
             shakemap_isosistas  un polígono por grado de intensidad MMI (ST_DumpAsPolygons)
        4. deja un resumen en .../almacenado.json, junto al COG.

Necesita rasterio (trae GDAL), numpy y psycopg: desplegar.py los empaqueta con la función.
"""
import hashlib
import json
import os
import tempfile
import urllib.parse
from datetime import datetime, timezone

import numpy as np
import psycopg
import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import Resampling, calculate_default_transform, reproject

from lambda_ingesta import _s3

BUCKET_SILVER = os.environ.get("BUCKET_SILVER", "sismo-silver")
SRID = 5361            # SIRGAS-Chile 2002 / UTM zona 19S
RESOLUCION_M = 1000
NODATA = -9999.0
# grilla del ShakeMap -> (banda, paso a unidades físicas): PGA y PGV vienen en logaritmo natural
CAPAS = {"mmi_mean": ("mmi", lambda v: v), "pga_mean": ("pga_g", np.exp), "pgv_mean": ("pgv_cm_s", np.exp)}
# grados MMI enteros: [1-2):1, [2-3):2, ..., [10-13]:10
RECLASIFICAR_MMI = ",".join(f"[{g}-{g + 1}):{g}" for g in range(1, 10)) + ",[10-13]:10"


def reproyectar(zip_local: str):
    """Grillas ESRI del ShakeMap (WGS84) -> arreglo [banda, fila, columna] en UTM 19S."""
    bandas = []
    for capa, (_, a_fisico) in CAPAS.items():
        with rasterio.open(f"zip://{zip_local}!{capa}.flt") as src:   # GDAL lee el .hdr del mismo zip
            crs = src.crs or "EPSG:4326"
            valores = a_fisico(src.read(1, masked=True).astype("float32").filled(np.nan))
            transform, ancho, alto = calculate_default_transform(
                crs, f"EPSG:{SRID}", src.width, src.height, *src.bounds, resolution=RESOLUCION_M)
            destino = np.full((alto, ancho), NODATA, dtype="float32")
            reproject(valores, destino, src_transform=src.transform, src_crs=crs, src_nodata=np.nan,
                      dst_transform=transform, dst_crs=f"EPSG:{SRID}", dst_nodata=NODATA,
                      resampling=Resampling.bilinear)
            bandas.append(destino)
    return np.stack(bandas), transform


def a_cog(bandas: np.ndarray, transform) -> bytes:
    perfil = {"driver": "COG", "count": len(bandas), "height": bandas.shape[1], "width": bandas.shape[2],
              "dtype": "float32", "crs": f"EPSG:{SRID}", "transform": transform, "nodata": NODATA,
              "compress": "DEFLATE"}
    with MemoryFile() as memoria:
        with memoria.open(**perfil) as cog:
            cog.write(bandas)
            cog.descriptions = tuple(nombre for nombre, _ in CAPAS.values())
        return memoria.read()


def guardar_en_postgis(meta: dict, cog: bytes, cog_s3: str, maximos: dict) -> dict:
    eid = meta["eventid"]
    # PGHOST, PGUSER, PGPASSWORD y PGDATABASE vienen de las variables de entorno de la Lambda
    with psycopg.connect() as con:  # una transacción: si algo falla, no queda nada a medias
        con.execute("""
            INSERT INTO eventos (event_id, magnitud, lugar, tiempo_utc, profundidad_km, epicentro)
            VALUES (%(id)s, %(mag)s, %(lugar)s, %(tiempo)s, %(prof)s,
                    ST_Transform(ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326), %(srid)s))
            ON CONFLICT (event_id) DO UPDATE SET magnitud = EXCLUDED.magnitud, lugar = EXCLUDED.lugar,
                tiempo_utc = EXCLUDED.tiempo_utc, profundidad_km = EXCLUDED.profundidad_km,
                epicentro = EXCLUDED.epicentro""",
            {"id": eid, "mag": meta.get("magnitud"), "lugar": meta.get("lugar"), "tiempo": meta.get("tiempo_utc"),
             "prof": meta.get("profundidad_km"), "lon": meta.get("longitud"), "lat": meta.get("latitud"),
             "srid": SRID})
        con.execute("DELETE FROM shakemaps WHERE event_id = %s", (eid,))  # borra en cascada teselas e isosistas
        con.execute("""
            INSERT INTO shakemaps (event_id, version_shakemap, cog_s3, cog_bytes, cog_sha256, resolucion_m,
                                   bandas, mmi_max, pga_max_g, pgv_max_cm_s)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (eid, str(meta.get("version_shakemap")), cog_s3, len(cog), hashlib.sha256(cog).hexdigest(),
             RESOLUCION_M, [nombre for nombre, _ in CAPAS.values()], *maximos.values()))
        con.execute("""
            INSERT INTO shakemap_teselas (event_id, rast)
            SELECT %s, ST_Tile(ST_FromGDALRaster(%s, %s), 100, 100)""", (eid, cog, SRID))
        con.execute("""
            INSERT INTO shakemap_isosistas (event_id, mmi, geom)
            SELECT %(id)s, val, ST_Multi(ST_Union(geom))
            FROM (SELECT (ST_DumpAsPolygons(ST_Reclass(rast, 1, %(expr)s, '8BUI', 0))).*
                  FROM shakemap_teselas WHERE event_id = %(id)s) AS poligonos
            GROUP BY val""", {"id": eid, "expr": RECLASIFICAR_MMI})
        teselas = con.execute("SELECT count(*) FROM shakemap_teselas WHERE event_id = %s", (eid,)).fetchone()[0]
        isosistas = con.execute("""SELECT mmi, round(ST_Area(geom) / 1e6) FROM shakemap_isosistas
                                   WHERE event_id = %s ORDER BY mmi""", (eid,)).fetchall()
    return {"teselas": teselas, "area_km2_por_mmi": {int(m): float(a) for m, a in isosistas}}


def almacenar(event, context=None):
    resultados = []
    for rec in event["Records"]:
        bucket = rec["s3"]["bucket"]["name"]
        clave_control = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        base = clave_control.rsplit("/", 1)[0]               # usgs_shakemap/evento=<id>
        s3 = _s3()
        control = json.loads(s3.get_object(Bucket=bucket, Key=clave_control)["Body"].read())
        if control["estado"] != "ok":
            print(f"[almacenar] {base}: el control está '{control['estado']}', no se almacena")
            continue
        meta = json.loads(s3.get_object(Bucket=bucket, Key=f"{base}/evento.json")["Body"].read())
        with tempfile.TemporaryDirectory() as tmp:
            zip_local = os.path.join(tmp, "raster.zip")
            s3.download_file(bucket, f"{base}/raster.zip", zip_local)
            bandas, transform = reproyectar(zip_local)
        maximos = {nombre: float(np.max(b[b != NODATA])) for (nombre, _), b in zip(CAPAS.values(), bandas)}
        cog = a_cog(bandas, transform)
        cog_s3 = f"s3://{BUCKET_SILVER}/{base}/shakemap_utm19s.tif"
        s3.put_object(Bucket=BUCKET_SILVER, Key=f"{base}/shakemap_utm19s.tif", Body=cog,
                      ContentType="image/tiff; application=geotiff; profile=cloud-optimized")
        resumen = {"eventid": meta["eventid"], "cog": cog_s3, "cog_bytes": len(cog), "srid": SRID,
                   "celdas": f"{bandas.shape[2]} x {bandas.shape[1]} de {RESOLUCION_M} m",
                   "maximos": {k: round(v, 3) for k, v in maximos.items()},
                   **guardar_en_postgis(meta, cog, cog_s3, maximos),
                   "almacenado_utc": datetime.now(timezone.utc).isoformat()}
        s3.put_object(Bucket=BUCKET_SILVER, Key=f"{base}/almacenado.json",
                      Body=json.dumps(resumen, indent=2).encode("utf-8"))
        print(f"[almacenar] {meta['eventid']}: COG {len(cog) / 1e6:.1f} MB -> {cog_s3} | PostGIS: "
              f"{resumen['teselas']} teselas, isosistas MMI {sorted(resumen['area_km2_por_mmi'])}")
        resultados.append(resumen)
    return resultados
