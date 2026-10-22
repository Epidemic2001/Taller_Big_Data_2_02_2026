"""Data Warehouse de los testimonios LastQuake en ClickHouse: modelo estrella cargado por Spark.

El job de Spark (job_spark.py) llama a cargar_lote() en cada micro-batch (foreachBatch). Los
testimonios nuevos se limpian, se cruzan con los datos del sismo y se reparten en el esquema
estrella de herramientas/clickhouse/esquema.sql:

  fact_felt_reports  un testimonio: intensidad, canal, distancia al epicentro, minutos desde el sismo
  dim_evento         el sismo según EMSC (magnitud, profundidad, región)
  dim_ubicacion      celda de 0,1° (~11 km) donde se sintió, con su macrozona de Chile
  dim_tiempo         la hora local de Chile del testimonio (fecha, día de la semana, franja)

Spark asegura que cada micro-batch se procese al menos una vez; como las tablas son
ReplacingMergeTree con claves deterministas, un reproceso no duplica filas.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import RAW  # noqa: E402

# Lo define docker-compose en el contenedor de Spark; sin él (Spark local) no se carga el warehouse
JDBC = {"url": os.environ.get("CLICKHOUSE_URL", ""), "user": os.environ.get("CLICKHOUSE_USER", ""),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver", "isolationLevel": "NONE"}  # ClickHouse no usa transacciones
ESQUEMA_EVENTO = StructType([
    StructField("unid", StringType()),
    StructField("tiempo_txt", StringType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType()),
    StructField("tipo_magnitud", StringType()),
    StructField("magnitud", DoubleType()),
    StructField("profundidad_km", DoubleType()),
    StructField("region", StringType()),
    StructField("n_testimonios", IntegerType()),
])
DIAS = ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"]  # orden de dayofweek()


def habilitado() -> bool:
    return bool(JDBC["url"])


def escribir(df: DataFrame, tabla: str) -> None:
    df.write.format("jdbc").options(**JDBC).option("dbtable", tabla).mode("append").save()


def a_timestamp(texto: str) -> Column:
    return F.to_timestamp(F.regexp_replace(texto, " UTC", ""), "yyyy-MM-dd'T'HH:mm:ss.SSS")


def distancia_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Distancia sobre la superficie de la Tierra (fórmula del haversine)."""
    a = (F.sin(F.radians(lat2 - lat1) / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(F.radians(lon2 - lon1) / 2) ** 2)
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(a))


def eventos(lote: DataFrame) -> DataFrame:
    """Datos del sismo (evento.csv que deja el sondeo en la capa cruda) de los sismos del micro-batch."""
    return (lote.sparkSession.read.schema(ESQUEMA_EVENTO).option("sep", "|").option("comment", "#")
            .csv(str(RAW / "emsc_lastquake" / "evento=*" / "evento.csv"))
            .join(lote.select("unid").distinct(), "unid")
            .dropDuplicates(["unid"])
            .withColumn("tiempo_utc", a_timestamp("tiempo_txt")))


def cargar_lote(lote: DataFrame, id_lote: int) -> None:
    """foreachBatch: reparte un micro-batch de testimonios en el modelo estrella de ClickHouse."""
    lote = lote.persist()
    if lote.isEmpty():
        lote.unpersist()
        return
    ev = eventos(lote).persist()
    # Limpieza de valores atípicos: intensidades fuera de la escala o coordenadas imposibles
    validos = lote.where(F.col("intensidad_cruda").between(1, 12) & F.col("intensidad_corregida").between(1, 12)
                         & F.col("latitud").between(-90, 90) & F.col("longitud").between(-180, 180)
                         & F.col("tiempo_utc").isNotNull())
    epicentro = ev.select("unid", F.col("latitud").alias("ev_lat"), F.col("longitud").alias("ev_lon"),
                          F.col("tiempo_utc").alias("ev_tiempo"))
    icorr = F.col("intensidad_corregida")
    hechos = (validos.join(epicentro, "unid", "left")
              .withColumn("report_id", F.xxhash64("unid", "longitud", "latitud", "tiempo_utc", "canal"))
              .withColumn("distancia_epicentro_km",
                          distancia_km(F.col("latitud"), F.col("longitud"), F.col("ev_lat"), F.col("ev_lon")))
              .withColumn("minutos_desde_sismo",
                          (F.col("tiempo_utc").cast("double") - F.col("ev_tiempo").cast("double")) / 60)
              .withColumn("local", F.from_utc_timestamp("tiempo_utc", "America/Santiago"))
              .withColumn("tiempo_key", F.date_format("local", "yyyyMMddHH").cast("int"))
              .withColumn("latitud_celda", F.round("latitud", 1))
              .withColumn("longitud_celda", F.round("longitud", 1))
              .withColumn("ubicacion_key", F.format_string("%.1f_%.1f", "latitud_celda", "longitud_celda"))
              # EMS-98: VI levemente dañino, VII dañino, VIII o más gravemente dañino
              .withColumn("nivel_danio_estimado", F.when(icorr >= 8, "grave").when(icorr >= 7, "moderado")
                          .when(icorr >= 6, "leve").otherwise("sin daño"))
              .persist())

    mag = F.col("magnitud")
    escribir(ev.select("unid", "tiempo_utc", "latitud", "longitud", "profundidad_km", "magnitud", "tipo_magnitud",
                       F.when(mag >= 6, "M6+").when(mag >= 5, "M5-6").when(mag >= 4, "M4-5")
                       .when(mag >= 3, "M3-4").otherwise("M<3").alias("clase_magnitud"),
                       "region", F.current_timestamp().alias("cargado_utc")), "dim_evento")
    escribir(hechos.select("ubicacion_key", "latitud_celda", "longitud_celda",
                           F.when(F.col("latitud_celda") > -27, "Norte Grande")
                           .when(F.col("latitud_celda") > -32.5, "Norte Chico")
                           .when(F.col("latitud_celda") > -38, "Zona Central")
                           .when(F.col("latitud_celda") > -44, "Zona Sur")
                           .otherwise("Zona Austral").alias("macrozona"))
             .dropDuplicates(["ubicacion_key"]), "dim_ubicacion")
    hora = F.hour("local")
    escribir(hechos.select("tiempo_key", F.to_date("local").alias("fecha"), hora.alias("hora_local"),
                           F.element_at(F.array(*map(F.lit, DIAS)), F.dayofweek("local")).alias("dia_semana"),
                           F.dayofweek("local").isin(1, 7).cast("int").alias("es_fin_de_semana"),
                           F.when(hora < 6, "madrugada").when(hora < 12, "mañana").when(hora < 20, "tarde")
                           .otherwise("noche").alias("franja"),
                           F.month("local").alias("mes"), F.year("local").alias("anio"))
             .dropDuplicates(["tiempo_key"]), "dim_tiempo")
    escribir(hechos.select("report_id", "unid", "ubicacion_key", "tiempo_key", "tiempo_utc", "canal",
                           "intensidad_cruda", "intensidad_corregida", "nivel_danio_estimado",
                           "distancia_epicentro_km", "minutos_desde_sismo", "procesado_utc"), "fact_felt_reports")

    n, total = hechos.count(), lote.count()
    print(f">>> [warehouse] micro-batch {id_lote}: {n} testimonios de {ev.count()} sismos -> ClickHouse "
          f"({total - n} descartados por valores atípicos)", flush=True)
    for df in (hechos, ev, lote):
        df.unpersist()
