# Arquitectura de Datos Sismológicos

Este proyecto implementa un *pipeline* de Big Data para la ingesta, procesamiento y análisis de datos sismológicos. La arquitectura integra catálogos históricos (procesamiento *batch*) y flujos de telemetría y alertas tempranas en tiempo real (*streaming*), obtenidos de las principales redes sismológicas globales y de Chile.

## 📡 Fuentes de datos y tecnologías de ingesta

| Fuente de datos | Propósito en el proyecto | Método y formato | Stack tecnológico |
| --- | --- | --- | --- |
| **USGS** (EE.UU.) | Data Lake histórico y entrenamiento de modelos. | API REST (FDSN) con consultas particionadas por fecha para no superar el límite de 20.000 resultados por consulta.<br>**Formato:** GeoJSON | Python (`requests`) + Apache Airflow |
| **EMSC** (Europa/Global) | Alertas tempranas en tiempo real (EEW). | Conexión asíncrona persistente (*streaming* puro).<br>**Formato:** JSON sobre WebSocket | Python (`websockets`) + Apache Kafka |
| **CSN** (Chile) | Datos crudos para *Deep Learning* (acelerómetros y GNSS). | Protocolo SeedLink para consumir flujos de trazas físicas.<br>**Formato:** `miniSEED` (binario) | ObsPy (Python) + Apache Kafka |
| **EMSC LastQuake** | Cruce de métricas con reportes de sismología ciudadana (*felt reports*). | API REST consultada por ID de evento para obtener testimonios ciudadanos de daños físicos. | Python (`requests`) |
| **USGS** (Shakemap) | Mapa de datos de sismos calibrada en la zona de Calama | Formato de Raster, representando una cuadricula de celdas en Bits de informacion visual oespacial | Python/ AWS Lamda + GDAL|

## ⚙️ Flujo general

1. Los contenedores de ingesta (productores) extraen los datos usando los protocolos descritos arriba.
2. Los formatos complejos como `miniSEED` se decodifican en el perímetro con ObsPy.
3. La información normalizada se publica de forma asíncrona en tópicos de Kafka, desde donde se procesa con Apache Flink o se almacena en el Data Lake.
