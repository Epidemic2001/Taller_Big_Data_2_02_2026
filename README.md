# Big Data en Sismología: ingesta de las 5 fuentes

Extractores de las 5 fuentes del proyecto. Cada uno guarda el archivo original en la
capa cruda y una versión en Parquet en la capa procesada:

```
datos/
├── raw/        capa cruda: el archivo tal como llega (XML, JSON, CSV, miniSEED, raster)
├── landing/    zona de llegada que lee Spark (testimonios LastQuake)
├── silver/     capa procesada: Parquet, particionado por fecha / evento / estación
└── _estado/    checkpoints de las cargas incrementales
```

## Cómo ejecutarlo (Windows, Anaconda Prompt)

```bat
cd %USERPROFILE%\universidad\big-data-sismologia
pip install -r requirements.txt
python ejecutar_todo.py
```

La primera ejecución tarda unos 10 minutos. Al final muestra cuántas filas quedaron por fuente.

Otras opciones:

```bat
python ejecutar_todo.py --solo csn usgs      :: solo algunas fuentes
python ejecutar_todo.py --minutos-emsc 15    :: escuchar EMSC más tiempo
python ejecutar_todo.py --spark              :: LastQuake con Spark real (requiere Java)
```

Spark en Windows además necesita `winutils.exe` de Hadoop. Es más simple correrlo en WSL o
Linux con `pip install pyspark==3.5.3`. Si Spark no encuentra el nombre del equipo, define
`SPARK_LOCAL_HOSTNAME=localhost` antes de ejecutar.

## Las 5 fuentes

| # | Fuente | Formato | Tipo de ingesta | Herramienta | Carpeta |
|---|---|---|---|---|---|
| 1 | EMSC Streaming | JSON | Streaming (WebSocket) | Apache Kafka | `fuentes/emsc_streaming` |
| 2 | USGS Catálogo | QuakeML (XML) | Batch por ventanas mensuales | Apache Airflow | `fuentes/usgs_catalogo` |
| 3 | CSN Chile (red C1) | miniSEED (binario) | Archivos masivos + incremental | Apache NiFi + ObsPy | `fuentes/csn_chile` |
| 4 | EMSC LastQuake | CSV | Micro-batching | Spark Structured Streaming | `fuentes/emsc_lastquake` |
| 5 | USGS ShakeMap | Raster ESRI Grid | Por evento (serverless) | AWS Lambda + GDAL | `fuentes/usgs_shakemap` |

### Qué extrae `ejecutar_todo.py` por defecto

| Fuente | Alcance por defecto | Resultado en `datos/silver/` |
|---|---|---|
| EMSC Streaming | 5 minutos escuchando sismos de todo el mundo | Un evento por fila (última versión) |
| USGS Catálogo | Chile, magnitud ≥ 2,5, últimos ~4 meses | Un sismo por fila, por año y mes |
| CSN Chile | **Masivo:** 25-09-2026 de 15:00 a 17:00 UTC (sismo M5,2 de O'Higgins), 6 estaciones. **Incremental:** la última hora | Una fila por ventana de 60 s, con la señal en m/s o m/s² |
| EMSC LastQuake | Sismos en Chile de los últimos 30 días | Un testimonio por fila, por sismo |
| USGS ShakeMap | Los 2 sismos más grandes en Chile desde 2026 (M ≥ 6) con ShakeMap | Un punto de la grilla (~1 km) por fila: MMI, PGA (g) y PGV (cm/s) |

## Las herramientas reales (Docker)

`docker-compose.yml` levanta las 5 herramientas, cada una con su perfil. La guía paso a paso
de la demo, con lo que hay que mostrar en cada interfaz, está en **[DEMO.md](DEMO.md)**.

| Herramienta | Levantar | Archivos |
|---|---|---|
| Kafka + Kafka UI | `docker compose --profile kafka up -d` | `fuentes/emsc_streaming/productor_kafka.py --kafka localhost:9092` |
| NiFi | `docker compose --profile nifi up -d` | `herramientas/nifi/crear_flujo.py` (crea el flujo por la API de NiFi) |
| Airflow | `docker compose --profile airflow up -d --build` | `herramientas/airflow/dags/usgs_catalogo_dag.py` + `fuentes/usgs_catalogo/validar.py` |
| Spark | `docker compose --profile spark up -d` | `fuentes/emsc_lastquake/job_spark.py` |
| Lambda + S3 (LocalStack) | `docker compose --profile lambda up -d` | `herramientas/lambda/` (`desplegar.py`, `invocar.py`, `lambda_ingesta.py`) |

`ejecutar_todo.py` usa un **modo local** equivalente, que no necesita Docker: los `.jsonl` hacen
de tópico Kafka, `extraer.py` recorre los meses en lugar de Airflow, `procesar_local.py` replica
el job de Spark con pandas, `csn_chile/extraer.py` hace lo que el flujo de NiFi, e
`invocar_local.py` llama las funciones Lambda en orden. La conversión con GDAL de
`fuentes/usgs_shakemap/lambda_function.py` corresponde al procesamiento (fase 4).

## Datos y citas

- **CSN Chile:** Universidad de Chile (2013). *Red Sismológica Nacional*. FDSN. https://doi.org/10.7914/SN/C1. Servidor: https://owl.csn.uchile.cl
- **EMSC (Streaming y LastQuake):** Euro-Mediterranean Seismological Centre, https://www.seismicportal.eu. Los testimonios se publican bajo CC BY 4.0.
- **USGS (Catálogo y ShakeMap):** U.S. Geological Survey, Earthquake Hazards Program, https://earthquake.usgs.gov. Unidades del raster: MMI lineal; PGA en ln(g) y PGV en ln(cm/s). El extractor las convierte a g y cm/s.
