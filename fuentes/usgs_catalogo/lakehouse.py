"""Almacenamiento del catálogo USGS en un Data Lakehouse con Apache Iceberg sobre MinIO.

Zonas del lakehouse (arquitectura medallón), todas en el bucket "lakehouse" de MinIO:
  bronze -> s3://lakehouse/bronze/usgs_catalogo/anio=AAAA/mes=MM/eventos.quakeml.xml.gz
            el QuakeML original de la API del USGS, comprimido y sin modificar
  silver -> tabla Iceberg silver.usgs_sismos: un registro por sismo con esquema tipado,
            particionada por mes del sismo (partición oculta) y ordenada por magnitud
  gold   -> tabla Iceberg gold.usgs_resumen_mensual: indicadores de cada mes calculados desde silver

Solo se cargan meses que pasaron el control de calidad (validar.py). Cada carga reemplaza
únicamente su mes (idempotente) y queda registrada como un snapshot de Iceberg con el mes y el
origen en su resumen. Eso habilita el Time Travel: leer la tabla tal como estaba en una carga
anterior, por ejemplo el corte exacto del catálogo con que se entrenó un modelo.

Uso directo (sin Airflow), con los meses ya extraídos y validados en datos/:
    python lakehouse.py --inicio 2026-07 --fin 2026-10
En Airflow, la tarea cargar_lakehouse llama a cargar_mes() después de validar_calidad.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import warnings
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyarrow import fs
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.transforms import IdentityTransform, MonthTransform

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import DATOS, RAW, SILVER, ahora_utc, log  # noqa: E402
from extraer import FUENTE, meses  # noqa: E402

# Desde Windows se llega por localhost; dentro de Docker (Airflow) por el nombre del servicio
S3_ENDPOINT = os.environ.get("LAKEHOUSE_S3_ENDPOINT", "http://localhost:9000")
CATALOGO_URI = os.environ.get("LAKEHOUSE_CATALOGO_URI", "http://localhost:8181")
S3_USUARIO, S3_CLAVE = "admin", "sismologia2026"
BUCKET = "lakehouse"
SILVER_TABLA = "silver.usgs_sismos"
GOLD_TABLA = "gold.usgs_resumen_mensual"

# La primera carga de un mes no tiene nada que reemplazar: PyIceberg lo avisa, pero es lo esperado
warnings.filterwarnings("ignore", message="Delete operation did not match any records")

UTC = pa.timestamp("us", tz="UTC")
ESQUEMA_SILVER = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("tiempo_utc", UTC, nullable=False),
    ("latitud", pa.float64()),
    ("longitud", pa.float64()),
    ("profundidad_km", pa.float64()),
    ("incert_profundidad_km", pa.float64()),
    ("magnitud", pa.float64()),
    ("tipo_magnitud", pa.string()),
    ("incert_magnitud", pa.float64()),
    ("lugar", pa.string()),
    ("tipo_evento", pa.string()),
    ("modo_evaluacion", pa.string()),
    ("n_estaciones", pa.int32()),
    ("gap_azimutal", pa.float64()),
    ("error_estandar_s", pa.float64()),
    ("agencia", pa.string()),
    ("archivo_bronze", pa.string()),   # linaje: de qué archivo bronze viene cada fila
    ("cargado_utc", UTC),
])
ESQUEMA_GOLD = pa.schema([
    pa.field("mes", pa.date32(), nullable=False),
    ("sismos", pa.int64()),
    ("magnitud_max", pa.float64()),
    ("magnitud_promedio", pa.float64()),
    ("sismos_m4_o_mas", pa.int64()),
    ("sismos_m5_o_mas", pa.int64()),
    ("superficiales_menos_70km", pa.int64()),
    ("profundidad_promedio_km", pa.float64()),
    ("snapshot_silver", pa.int64()),   # linaje: versión de silver desde la que se calculó la fila
    ("calculado_utc", UTC),
])


def catalogo():
    return load_catalog("lakehouse", type="rest", uri=CATALOGO_URI, **{
        "s3.endpoint": S3_ENDPOINT, "s3.access-key-id": S3_USUARIO,
        "s3.secret-access-key": S3_CLAVE, "s3.region": "us-east-1"})


def minio() -> fs.S3FileSystem:
    s3 = fs.S3FileSystem(endpoint_override=S3_ENDPOINT, access_key=S3_USUARIO, secret_key=S3_CLAVE,
                         region="us-east-1", allow_bucket_creation=True)
    if s3.get_file_info(BUCKET).type == fs.FileType.NotFound:
        s3.create_dir(BUCKET)
    return s3


def filtro_mes(anio: int, mes: int):
    fin = date(anio + (mes == 12), mes % 12 + 1, 1)
    return And(GreaterThanOrEqual("tiempo_utc", f"{date(anio, mes, 1)}T00:00:00+00:00"),
               LessThan("tiempo_utc", f"{fin}T00:00:00+00:00"))


def tabla(cat, nombre: str, esquema: pa.Schema, particionar_por_mes: bool = False):
    """Abre la tabla Iceberg o la crea la primera vez."""
    try:
        return cat.load_table(nombre)
    except NoSuchTableError:
        pass
    cat.create_namespace_if_not_exists(nombre.split(".")[0])
    with cat.create_table_transaction(nombre, schema=esquema) as tx:
        if particionar_por_mes:
            with tx.update_spec() as spec:      # partición oculta: se consulta por tiempo_utc, no por "mes"
                spec.add_field("tiempo_utc", MonthTransform(), "mes")
            with tx.update_sort_order() as orden:
                orden.desc("magnitud", IdentityTransform())
    log(FUENTE, f"Tabla Iceberg creada: {nombre}")
    return cat.load_table(nombre)


def mes_validado(anio: int, mes: int) -> bool:
    control = DATOS / "_control" / FUENTE / f"anio={anio}" / f"mes={mes:02d}" / "control.json"
    return control.exists() and json.loads(control.read_text())["estado"] == "ok"


def subir_bronze(anio: int, mes: int) -> str:
    origen = RAW / FUENTE / f"anio={anio}" / f"mes={mes:02d}" / "eventos.quakeml.xml"
    destino = f"{BUCKET}/bronze/{FUENTE}/anio={anio}/mes={mes:02d}/eventos.quakeml.xml.gz"
    with minio().open_output_stream(destino, compression="gzip") as f:
        f.write(origen.read_bytes())
    return f"s3://{destino}"


def cargar_silver(cat, anio: int, mes: int, archivo_bronze: str, propiedades: dict) -> int:
    df = pd.read_parquet(SILVER / FUENTE / f"anio={anio}" / f"mes={mes:02d}" / "eventos.parquet")
    df["tiempo_utc"] = pd.to_datetime(df["tiempo_utc"], utc=True)
    df["archivo_bronze"] = archivo_bronze
    df["cargado_utc"] = ahora_utc()
    # PyIceberg no aplica el orden de la tabla al escribir, así que se ordena aquí
    datos = pa.Table.from_pandas(df, schema=ESQUEMA_SILVER, preserve_index=False).sort_by(
        [("magnitud", "descending")])
    tabla(cat, SILVER_TABLA, ESQUEMA_SILVER, particionar_por_mes=True).overwrite(
        datos, overwrite_filter=filtro_mes(anio, mes), snapshot_properties=propiedades)
    return datos.num_rows


def cargar_gold(cat, anio: int, mes: int, propiedades: dict) -> dict:
    silver = cat.load_table(SILVER_TABLA)
    df = silver.scan(row_filter=filtro_mes(anio, mes),
                     selected_fields=("magnitud", "profundidad_km")).to_pandas()
    fila = {
        "mes": date(anio, mes, 1),
        "sismos": len(df),
        "magnitud_max": float(df["magnitud"].max()),
        "magnitud_promedio": round(float(df["magnitud"].mean()), 2),
        "sismos_m4_o_mas": int((df["magnitud"] >= 4).sum()),
        "sismos_m5_o_mas": int((df["magnitud"] >= 5).sum()),
        "superficiales_menos_70km": int((df["profundidad_km"] < 70).sum()),
        "profundidad_promedio_km": round(float(df["profundidad_km"].mean()), 1),
        "snapshot_silver": silver.current_snapshot().snapshot_id,
        "calculado_utc": ahora_utc(),
    }
    tabla(cat, GOLD_TABLA, ESQUEMA_GOLD).overwrite(
        pa.Table.from_pylist([fila], schema=ESQUEMA_GOLD),
        overwrite_filter=EqualTo("mes", fila["mes"].isoformat()), snapshot_properties=propiedades)
    return fila


def cargar_mes(anio: int, mes: int, origen: str = "manual") -> dict:
    """Tarea idempotente de un mes (bronze -> silver -> gold): la llama Airflow en cada ejecución."""
    if not mes_validado(anio, mes):
        log(FUENTE, f"Lakehouse {anio}-{mes:02d}: sin datos validados, no se carga")
        return {"anio": anio, "mes": mes, "sismos": 0}
    cat = catalogo()
    archivo_bronze = subir_bronze(anio, mes)
    propiedades = {"mes": f"{anio}-{mes:02d}", "origen": origen, "archivo_bronze": archivo_bronze}
    n = cargar_silver(cat, anio, mes, archivo_bronze, propiedades)
    resumen = cargar_gold(cat, anio, mes, propiedades)
    log(FUENTE, f"Lakehouse {anio}-{mes:02d}: bronze {archivo_bronze} | {n} sismos -> {SILVER_TABLA} "
                f"(snapshot {resumen['snapshot_silver']}) | resumen -> {GOLD_TABLA}")
    return {"anio": anio, "mes": mes, "sismos": n, "snapshot_silver": resumen["snapshot_silver"]}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--inicio", default="2026-07", help="mes inicial AAAA-MM")
    ap.add_argument("--fin", default="2026-10", help="mes final AAAA-MM (incluido)")
    a = ap.parse_args()
    total = sum(cargar_mes(an, me)["sismos"] for an, me in meses(a.inicio, a.fin))
    log(FUENTE, f"Total: {total} sismos en el lakehouse")
