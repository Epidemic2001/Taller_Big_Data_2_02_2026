"""Consultas analíticas (OLAP) sobre el Data Warehouse de testimonios LastQuake en ClickHouse.

Usa la interfaz HTTP de ClickHouse (puerto 8123); las mismas consultas se pueden pegar en el editor
web http://localhost:8123/play (usuario admin, clave sismologia).

    python consultar_warehouse.py
"""
from __future__ import annotations

import io

import pandas as pd
import requests

CLICKHOUSE = "http://localhost:8123/"
USUARIO, CLAVE = "admin", "sismologia"

pd.set_option("display.width", 200)

CONSULTAS = {
    "Contenido del modelo estrella (filas sin duplicados)": """
        SELECT 'fact_felt_reports' AS tabla, count() AS filas FROM fact_felt_reports FINAL
        UNION ALL SELECT 'dim_evento', count() FROM dim_evento FINAL
        UNION ALL SELECT 'dim_ubicacion', count() FROM dim_ubicacion FINAL
        UNION ALL SELECT 'dim_tiempo', count() FROM dim_tiempo FINAL""",

    "Sismos con más testimonios": """
        SELECT unid, region, magnitud, count() AS testimonios,
               max(intensidad_corregida) AS intensidad_max, round(max(distancia_epicentro_km)) AS km_max
        FROM v_testimonios GROUP BY unid, region, magnitud ORDER BY testimonios DESC LIMIT 8""",

    "Atenuación: intensidad promedio según distancia al epicentro y magnitud": """
        SELECT multiIf(distancia_epicentro_km < 50, '1. <50 km', distancia_epicentro_km < 100, '2. 50-100 km',
                       distancia_epicentro_km < 200, '3. 100-200 km', '4. 200+ km') AS distancia,
               round(avgIf(intensidad_corregida, clase_magnitud = 'M3-4'), 1) AS `M3-4`,
               round(avgIf(intensidad_corregida, clase_magnitud = 'M4-5'), 1) AS `M4-5`,
               round(avgIf(intensidad_corregida, clase_magnitud = 'M5-6'), 1) AS `M5-6`,
               round(avgIf(intensidad_corregida, clase_magnitud = 'M6+'), 1) AS `M6+`,
               count() AS testimonios
        FROM v_testimonios WHERE distancia_epicentro_km IS NOT NULL
        GROUP BY distancia ORDER BY distancia""",

    "Dónde se sintió: macrozona": """
        SELECT macrozona, count() AS testimonios, round(avg(intensidad_corregida), 1) AS intensidad_prom,
               countIf(nivel_danio_estimado != 'sin daño') AS con_danio_estimado
        FROM v_testimonios GROUP BY macrozona ORDER BY testimonios DESC""",

    "Cuándo se reporta: franja horaria (hora de Chile) y rapidez de la respuesta ciudadana": """
        SELECT franja, count() AS testimonios,
               round(quantile(0.5)(minutos_desde_sismo), 1) AS mediana_minutos_tras_sismo
        FROM v_testimonios GROUP BY franja ORDER BY testimonios DESC""",

    "Canal por el que llegan los testimonios": """
        SELECT canal, count() AS testimonios, round(100 * count() / sum(count()) OVER (), 1) AS porcentaje
        FROM v_testimonios GROUP BY canal ORDER BY testimonios DESC""",
}


def consultar(sql: str) -> pd.DataFrame:
    r = requests.post(CLICKHOUSE, params={"database": "sismologia"}, auth=(USUARIO, CLAVE),
                      data=f"{sql} FORMAT CSVWithNames".encode(), timeout=60)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


if __name__ == "__main__":
    for titulo, sql in CONSULTAS.items():
        print(f"\n=== {titulo}")
        print(consultar(sql).to_string(index=False))
