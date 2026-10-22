"""Job de Spark Structured Streaming para los testimonios LastQuake (micro-batching).

Lee la zona de llegada datos/landing/lastquake/*.csv como un flujo: cada vez que
el sondeo deja archivos nuevos, Spark los procesa en el siguiente micro-batch con
dos consultas independientes (cada una con su checkpoint):
  lastquake_silver     -> los agrega al Parquet de la capa silver, particionado por sismo
  lastquake_warehouse  -> los carga en el modelo estrella de ClickHouse (warehouse.py);
                          solo si está definido CLICKHOUSE_URL, como en el contenedor de Spark

Uso (requiere Java + pyspark):
    python job_spark.py --una-vez        # procesa lo pendiente y termina (availableNow)
    python job_spark.py                  # queda escuchando, micro-batch cada 1 minuto
"""
import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import DATOS, LANDING, SILVER  # noqa: E402
import warehouse  # noqa: E402

ESQUEMA = StructType([
    StructField("longitud", DoubleType()),
    StructField("latitud", DoubleType()),
    StructField("tiempo_txt", StringType()),
    StructField("canal", StringType()),             # app / mob / tel / web
    StructField("intensidad_cruda", IntegerType()),
    StructField("intensidad_corregida", DoubleType()),
])


def main(una_vez: bool):
    # las horas de LastQuake vienen en UTC: se interpretan así aunque el equipo esté en hora de Chile
    spark = (SparkSession.builder.appName("lastquake-microbatch")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    (LANDING / "lastquake").mkdir(parents=True, exist_ok=True)
    if not una_vez:
        print(">>> Spark escuchando datos/landing/lastquake (micro-batch cada 1 minuto). "
              "Spark UI: http://localhost:4040", flush=True)
    flujo = (spark.readStream.schema(ESQUEMA)
             .option("comment", "#")                # las 4 primeras líneas son comentarios
             .option("maxFilesPerTrigger", 50)
             .csv(str(LANDING / "lastquake")))
    limpio = (flujo
              .withColumn("archivo", F.input_file_name())
              .withColumn("unid", F.regexp_extract("archivo", r"(\d{8}_\d{7})__", 1))
              .withColumn("tiempo_utc", F.to_timestamp(F.regexp_replace("tiempo_txt", " UTC", ""),
                                                       "yyyy-MM-dd'T'HH:mm:ss.SSS"))
              .withColumn("procesado_utc", F.current_timestamp())
              .drop("tiempo_txt", "archivo"))
    escritores = [limpio.writeStream.queryName("lastquake_silver").format("parquet")
                  # carpeta propia de Spark: el modo local (pandas) escribe en silver/emsc_lastquake
                  .option("path", str(SILVER / "emsc_lastquake_spark"))
                  .option("checkpointLocation", str(DATOS / "_estado" / "spark_lastquake"))
                  .partitionBy("unid")]
    if warehouse.habilitado():
        escritores.append(limpio.writeStream.queryName("lastquake_warehouse")
                          .foreachBatch(warehouse.cargar_lote)
                          .option("checkpointLocation", str(DATOS / "_estado" / "spark_lastquake_warehouse")))
    consultas = [(e.trigger(availableNow=True) if una_vez else e.trigger(processingTime="1 minute")).start()
                 for e in escritores]
    if una_vez:
        for c in consultas:
            c.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--una-vez", action="store_true")
    main(ap.parse_args().una_vez)
