# Imagen de Airflow con las librerías que usa la extracción del catálogo USGS (ObsPy para QuakeML).
# Se fija la versión de Airflow para que pip no la actualice al instalar lo demás.
FROM apache/airflow:2.10.5-python3.11
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    "obspy>=1.4" "pandas>=2.0" "pyarrow>=14" "requests>=2.31"
