"""Consultas espaciales sobre la base geoespacial del ShakeMap (PostgreSQL + PostGIS).

Las mismas consultas se pueden ejecutar en psql:
    docker exec -it sismo-postgis psql -U admin -d sismologia

    python consultar_postgis.py
"""
from __future__ import annotations

import pandas as pd
import psycopg

from infraestructura_calama import POSTGIS

pd.set_option("display.width", 220)
pd.set_option("display.max_colwidth", 45)

CALAMA = "ST_Transform(ST_SetSRID(ST_MakePoint(-68.929, -22.456), 4326), 5361)"  # plaza de Calama

CONSULTAS = {
    "Sismos y sus ShakeMap (el archivo pesado, el COG, está en S3)": """
        SELECT e.event_id, e.magnitud, e.lugar, s.mmi_max, round(s.pga_max_g::numeric, 3) AS pga_max_g,
               s.cog_s3, round(s.cog_bytes / 1e6, 1) AS cog_mb,
               (SELECT count(*) FROM shakemap_teselas t WHERE t.event_id = e.event_id) AS teselas
        FROM eventos e JOIN shakemaps s USING (event_id) ORDER BY e.magnitud DESC""",

    "Raster en PostGIS: teselas, SRID y tamaño de celda": """
        SELECT event_id, count(*) AS teselas, min(ST_SRID(rast)) AS srid, min(ST_NumBands(rast)) AS bandas,
               min(ST_ScaleX(rast)) AS celda_m, max(ST_Width(rast)) || ' x ' || max(ST_Height(rast)) AS tesela
        FROM shakemap_teselas GROUP BY event_id""",

    "Área expuesta a cada grado de intensidad (isosistas, km²)": """
        SELECT event_id, mmi, round(ST_Area(geom) / 1e6) AS area_km2 FROM shakemap_isosistas
        ORDER BY event_id, mmi DESC""",

    "ST_Value: intensidad en cada instalación crítica de Calama (las 10 más expuestas por sismo)": """
        SELECT event_id, tipo, nombre, mmi, pga_g, pgv_cm_s, distancia_epicentro_km
        FROM (SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY mmi DESC NULLS LAST) AS n
              FROM v_exposicion_infraestructura WHERE nombre IS NOT NULL) x
        WHERE n <= 10 ORDER BY event_id, mmi DESC""",

    "ST_Intersects: instalaciones dentro de polígonos de intensidad MMI >= 6 (daño leve o más)": """
        SELECT s.event_id, i.tipo, count(*) AS instalaciones,
               string_agg(DISTINCT i.nombre, ', ') FILTER (WHERE i.nombre IS NOT NULL) AS ejemplos
        FROM shakemap_isosistas s JOIN infraestructura_calama i ON ST_Intersects(s.geom, i.geom)
        WHERE s.mmi >= 6 GROUP BY s.event_id, i.tipo ORDER BY s.event_id, instalaciones DESC""",

    "ST_Clip: estadística de MMI dentro de 20 km de la plaza de Calama": f"""
        SELECT event_id, round((stats).min::numeric, 1) AS mmi_min, round((stats).mean::numeric, 1) AS mmi_prom,
               round((stats).max::numeric, 1) AS mmi_max, (stats).count AS celdas_de_1km
        FROM (SELECT event_id, ST_SummaryStatsAgg(ST_Clip(rast, 1, ST_Buffer({CALAMA}, 20000), true), 1, true) AS stats
              FROM shakemap_teselas WHERE ST_Intersects(rast, ST_Buffer({CALAMA}, 20000))
              GROUP BY event_id) x""",
}


def main() -> None:
    with psycopg.connect(POSTGIS) as con:
        for titulo, sql in CONSULTAS.items():
            cur = con.execute(sql)
            filas = cur.fetchall()
            print(f"\n=== {titulo}")
            print(pd.DataFrame(filas, columns=[c.name for c in cur.description]).to_string(index=False)
                  if filas else "(sin resultados)")


if __name__ == "__main__":
    main()
