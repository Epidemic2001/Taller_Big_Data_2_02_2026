"""DAG de Apache Airflow: ingesta mensual del catálogo sísmico del USGS (QuakeML).

Tareas de cada ejecución mensual:
  extraer_quakeml   -> descarga el mes desde la API FDSN del USGS y guarda el XML (capa bronce)
  validar_calidad   -> controles de calidad; si hay más de 5 % de registros inválidos, falla
  cargar_lakehouse  -> almacena el mes en el Data Lakehouse (MinIO + Apache Iceberg):
                       QuakeML en bronze, tabla silver.usgs_sismos y tabla gold.usgs_resumen_mensual

catchup=True: al activar el DAG, Airflow crea una ejecución por cada mes desde
start_date (backfill del histórico). retries=3: reintenta si la API falla.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("SISMO_HOME", "/opt/sismo"), "fuentes", "usgs_catalogo"))
from extraer import extraer_mes  # noqa: E402
from lakehouse import cargar_mes  # noqa: E402
from validar import validar_mes  # noqa: E402


def _extraer(data_interval_start, **_):
    return extraer_mes(data_interval_start.year, data_interval_start.month, minmag=2.5)


def _validar(data_interval_start, **_):
    return validar_mes(data_interval_start.year, data_interval_start.month)


def _cargar(data_interval_start, run_id, **_):
    # el run_id queda en el snapshot de Iceberg: se sabe qué ejecución de Airflow creó cada versión
    return cargar_mes(data_interval_start.year, data_interval_start.month, origen=f"airflow {run_id}")


with DAG(
    dag_id="usgs_catalogo_chile",
    description="Catálogo sísmico USGS de Chile, mes a mes (QuakeML)",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=2,
    default_args={"owner": "equipo-sismologia", "retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["sismologia", "ingesta", "batch", "quakeml"],
) as dag:
    extraer = PythonOperator(task_id="extraer_quakeml", python_callable=_extraer)
    validar = PythonOperator(task_id="validar_calidad", python_callable=_validar)
    cargar = PythonOperator(task_id="cargar_lakehouse", python_callable=_cargar)
    extraer >> validar >> cargar
