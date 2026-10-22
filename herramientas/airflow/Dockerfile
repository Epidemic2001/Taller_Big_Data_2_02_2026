# Imagen de Airflow con las librerías que usa el catálogo USGS: ObsPy para QuakeML (extracción)
# y PyIceberg para el Data Lakehouse (almacenamiento).
# Se fija la versión de Airflow para que pip no la actualice al instalar lo demás, y se acota
# pyarrow: PyIceberg pide >=18 y las versiones más nuevas exigen NumPy 2 (Airflow 2.10 trae 1.26).
FROM apache/airflow:2.10.5-python3.11
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    "obspy>=1.4" "pandas>=2.0" "pyarrow>=18,<24" "requests>=2.31" \
    "pyiceberg[pyarrow,pyiceberg-core]>=0.10"
