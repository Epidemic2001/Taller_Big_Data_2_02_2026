# Guía de demo: las 5 herramientas de ingesta

Todo se ejecuta desde la carpeta del proyecto, en la terminal Bash de VS Code y con el `.venv` activo:

```bash
cd ~/universidad/big-data-sismologia
source .venv/Scripts/activate
pip install -r requirements.txt        # una vez (agrega kafka-python, boto3, pymongo, pyiceberg y zarr)
```

**Requisito:** Docker Desktop abierto. Levanten **una herramienta a la vez**, porque juntas usan
unos 7 GB de RAM. Para bajar una: `docker compose --profile <nombre> down`.

| # | Herramienta | Fuente | Formato | Interfaz | Usuario / clave |
|---|---|---|---|---|---|
| 1 | Kafka | EMSC Streaming | JSON | http://localhost:8081 | — |
| 2 | NiFi | CSN Chile | miniSEED | https://localhost:8443/nifi | admin / sismologia2026 |
| 3 | Airflow | USGS Catálogo | QuakeML (XML) | http://localhost:8080 | admin / sismologia |
| 4 | Spark Structured Streaming | EMSC LastQuake | CSV | http://localhost:4040 | — |
| 5 | AWS Lambda + S3 (LocalStack) | USGS ShakeMap | Raster | (consola) | — |

En cada demo sigan el mismo orden: **fuente original → herramienta funcionando → dato guardado + control**.

---

## 1. Kafka: streaming en tiempo real (EMSC)

```bash
docker compose --profile kafka up -d
python fuentes/emsc_streaming/productor_kafka.py --kafka localhost:9092 --duracion 900
```

**Qué mostrar:**
1. Kafka UI (http://localhost:8081) → *Topics* → `sismos-emsc` → *Messages*. Cada sismo que ocurre
   en el mundo aparece en segundos, como JSON. Muestren un mensaje `create` y su `update` posterior
   (EMSC corrige la magnitud).
2. La terminal del productor imprime cada sismo apenas llega: **el servidor empuja los datos por
   WebSocket, no hay polling**.
3. Consumidor que lee el tópico y deja la última versión de cada sismo:
   `python fuentes/emsc_streaming/consumidor_parquet.py --kafka localhost:9092`
4. Capa bronce: `datos/raw/emsc_streaming/fecha=.../mensajes.jsonl` (JSON original).

Pueden pasar varios minutos sin sismos. Dejen el productor corriendo desde antes de la presentación.

---

## 2. NiFi: ingesta de archivos masivos (CSN Chile)

```bash
docker compose --profile nifi up -d
python herramientas/nifi/crear_flujo.py          # espera a que NiFi arranque (1-2 min) y crea el flujo
```

Abran https://localhost:8443/nifi. El navegador avisa que el certificado no es confiable:
*Avanzado → Continuar*. Entren con admin / sismologia2026 y hagan doble clic en el grupo
**"Ingesta CSN Chile - miniSEED"**.

**Qué mostrar:**
1. El flujo completo en el lienzo: disparo cada 5 min → una estación por FlowFile → ventana de
   tiempo → **GET al servicio FDSN del CSN** → **validación** (HTTP 200 y archivo no vacío) →
   guardar el miniSEED en disco y en el **Data Lake de MinIO** (fase de almacenamiento, sección 8).
2. Seleccionen todo (Ctrl+A) y aprieten **Play**. Las colas se llenan y los contadores avanzan en vivo.
3. Clic derecho en "8. Guardar miniSEED" → **View data provenance** → abran un evento → pestaña
   *Lineage*: el recorrido completo del archivo. Esto es la **trazabilidad** que pide la pauta.
4. Clic derecho en una cola → *List queue* → *View content*: el miniSEED binario original.
5. Los archivos aparecen en `datos/raw/csn_chile/nifi/estacion=.../fecha=.../`.
6. Las estaciones sin datos terminan en **"Control: sin datos o errores"** (no se pierden en silencio).

Si se equivocan, borren el grupo en NiFi (clic derecho → Delete) y vuelvan a ejecutar `crear_flujo.py`.

---

## 3. Airflow: batch orquestado (USGS Catálogo)

```bash
docker compose --profile airflow up -d --build   # la primera vez construye la imagen (~5 min)
```

Abran http://localhost:8080 (admin / sismologia). Pueden pasar 1-2 minutos hasta que aparezca el DAG.

**Qué mostrar:**
1. El DAG **`usgs_catalogo_chile`** y su grafo: `extraer_quakeml → validar_calidad → cargar_lakehouse`
   (la tercera tarea es la fase de almacenamiento, sección 7).
2. **Actívenlo** con el interruptor (está pausado). Por `catchup=True`, Airflow crea una
   ejecución **por cada mes desde enero de 2026**: este es el **backfill** del histórico, que
   respeta el límite de 20.000 eventos por consulta de la API.
3. Vista *Grid*: las casillas se ponen verdes mes a mes. Abran una tarea → *Logs*: se ve la descarga
   y el reporte de calidad.
4. Archivos: el QuakeML original en `datos/raw/usgs_catalogo/anio=2026/mes=MM/` y el control de
   calidad en `datos/_control/usgs_catalogo/.../control.json`.
5. Retries: si la API falla, la tarea se reintenta 3 veces sola (casilla amarilla *up_for_retry*).

---

## 4. Spark Structured Streaming: micro-batch (EMSC LastQuake)

Terminal 1 (Spark queda escuchando la carpeta de llegada):
```bash
docker compose --profile spark up -d
docker logs -f sismo-spark
```
Terminal 2 (deja testimonios nuevos en la zona de llegada):
```bash
python fuentes/emsc_lastquake/sondeo.py --desde 2026-09-01 --ciclos 1
```

**Qué mostrar:**
1. Los CSV originales llegando a `datos/landing/lastquake/` (formato original de LastQuake).
2. Spark UI (http://localhost:4040) → pestaña **Structured Streaming** → la consulta activa:
   cada **micro-batch** (cada 1 minuto), cuántas filas entraron y la tasa de procesamiento.
3. Resultado en `datos/silver/emsc_lastquake_spark/unid=.../`. El `_spark_metadata` y el checkpoint
   (`datos/_estado/spark_lastquake`) garantizan que un archivo **nunca se procese dos veces**.
4. Ejecuten otro ciclo de sondeo y muestren el siguiente micro-batch en la UI.

---

## 5. AWS Lambda + S3: ingesta por eventos (USGS ShakeMap)

```bash
docker compose --profile lambda up -d
python herramientas/lambda/desplegar.py      # crea el bucket, las 2 Lambdas y el disparador S3
python herramientas/lambda/invocar.py        # invoca la Lambda por cada sismo M>=6 de 2026
```

**Antes de presentar,** ejecuten `invocar.py` una vez: la primera invocación tarda 1-2 minutos
porque LocalStack descarga el runtime de Python.

**Qué mostrar:**
1. `shakemap-descargar` se invoca con el id del sismo, busca su ShakeMap en la API del USGS y sube
   `raster.zip` a S3 (`s3://sismo-bronze/usgs_shakemap/evento=.../`).
2. **Nadie llama a `shakemap-validar`:** la **activa S3 sola** al llegar el archivo (evento
   ObjectCreated). Eso es la ingesta basada en eventos / serverless.
3. El control que escribe la validación: grillas presentes, tamaño esperado según el `.hdr`, hash
   SHA-256. Queda en `_control.json` y se copia a `datos/raw/usgs_shakemap/`.
4. Logs de ambas Lambdas: `docker logs sismo-localstack`.

Si LocalStack no arranca o pide un *auth token*: creen una cuenta gratuita en localstack.cloud,
copien su token y creen un archivo `.env` en la carpeta del proyecto con
`LOCALSTACK_AUTH_TOKEN=su_token`.

---

# Fase de almacenamiento

## 6. MongoDB: almacenamiento NoSQL de las alertas EMSC (Kafka → MongoDB)

El perfil `kafka` también levanta **MongoDB** y **Mongo Express**. Con el productor de la sección 1
corriendo, en otra terminal:

```bash
python fuentes/emsc_streaming/consumidor_mongodb.py --kafka localhost:9092 --continuo
```

Sin `--continuo` procesa lo pendiente del tópico y termina. Sin `--kafka` carga los `.jsonl` de la
capa cruda: sirve para recuperar ingestas anteriores, porque el tópico de Kafka se borra con `down`.

**Qué mostrar:**
1. Cada sismo que imprime el productor aparece segundos después en la terminal del consumidor
   (`MongoDB <- create ...`).
2. Mongo Express (http://localhost:8082, admin / sismologia) → base `sismologia`:
   - `emsc_eventos`: **un documento por sismo** con su versión más reciente (`_id` = `unid`).
   - `emsc_revisiones`: el **historial**: el `create` y cada `update` de EMSC, con el JSON original
     en `mensaje_original`.
3. Por qué NoSQL: el documento guarda el JSON tal como llega y cada corrección de magnitud se agrega
   sin cambiar ningún esquema. Un `update` atrasado nunca pisa una versión más nueva.
4. Índices: compuesto `(unid, ultima_actualizacion_utc desc)` y geoespacial `2dsphere` sobre
   `epicentro` (GeoJSON).
5. Consultas en vivo con `mongosh` dentro del contenedor:

```bash
docker exec -it sismo-mongodb mongosh -u admin -p sismologia --authenticationDatabase admin --quiet sismologia
```

```js
db.emsc_eventos.countDocuments()
// los 5 sismos más recientes
db.emsc_eventos.find({}, {magnitud: 1, region: 1, tiempo_utc: 1}).sort({tiempo_utc: -1}).limit(5)
// sismos a menos de 500 km de Santiago (usa el índice 2dsphere)
db.emsc_eventos.find({epicentro: {$nearSphere: {$geometry: {type: "Point", coordinates: [-70.65, -33.45]},
                                                $maxDistance: 500000}}}, {magnitud: 1, region: 1})
// historial de revisiones de un sismo (cambien el unid por uno de su base)
db.emsc_revisiones.find({unid: "20261021_0000333"}, {accion: 1, magnitud: 1, ultima_actualizacion_utc: 1})
                  .sort({ultima_actualizacion_utc: -1})
```

Los datos quedan en el volumen de Docker `mongo-datos` y **sobreviven a
`docker compose --profile kafka down`**; solo `down -v` los borra. Para consultar lo guardado sin
prender Kafka: `docker compose --profile mongo up -d`.

---

## 7. Data Lakehouse con Apache Iceberg: catálogo USGS (Airflow → MinIO + Iceberg)

El perfil `airflow` también levanta **MinIO** (almacenamiento de objetos S3) y el **catálogo REST de
Apache Iceberg**. No hay que ejecutar nada aparte: la tarea `cargar_lakehouse` del DAG almacena cada
mes después de que pasa el control de calidad.

```bash
docker compose --profile airflow up -d --build
```

| Zona | Dónde queda | Qué es |
|---|---|---|
| **Bronze** | `s3://lakehouse/bronze/usgs_catalogo/anio=AAAA/mes=MM/eventos.quakeml.xml.gz` | El QuakeML original, comprimido y sin modificar |
| **Silver** | Tabla Iceberg `silver.usgs_sismos` | Un registro por sismo, esquema tipado, particionada por mes y ordenada por magnitud |
| **Gold** | Tabla Iceberg `gold.usgs_resumen_mensual` | Indicadores por mes (cantidad, magnitud máx., M≥4, M≥5, superficiales) calculados desde silver |

**Qué mostrar:**
1. En Airflow, la tarea `cargar_lakehouse` de un mes → *Logs*: sube el bronze, escribe silver y
   recalcula gold.
2. Consola de MinIO (http://localhost:9001, admin / sismologia2026) → bucket `lakehouse`:
   - `bronze/`: los QuakeML comprimidos.
   - `warehouse/silver/usgs_sismos/data/mes=2026-01/...`: los **Parquet**, uno por partición.
   - `warehouse/.../metadata/`: los **metadatos de Iceberg** (`metadata.json`, manifiestos `.avro`).
     Esto es lo que convierte un Data Lake en un *Lakehouse*: transacciones ACID, esquema y versiones
     sobre archivos abiertos.
3. Contenido, versiones y Time Travel desde Windows:
   ```bash
   python fuentes/usgs_catalogo/consultar_lakehouse.py
   ```
   - **Gold:** un resumen por mes, con el `snapshot_silver` desde el que se calculó (linaje).
   - **Particiones ocultas:** Iceberg particiona por mes de `tiempo_utc` sin una columna extra.
   - **Historial de snapshots:** cada carga es una versión, con el mes y el `run_id` de Airflow que la
     creó. Si se vuelve a ejecutar un mes (*Clear* en Airflow) aparece un `delete` + `append` del
     mismo mes y el total no cambia: la carga es **idempotente**.
   - **Time Travel:** la tabla leída en su primer snapshot versus la actual. Es la forma de saber con qué
     corte exacto del catálogo se entrenó un modelo. Para comparar con otra versión:
     `python fuentes/usgs_catalogo/consultar_lakehouse.py --snapshot <snapshot_id>`
4. Sin Airflow, con los meses ya extraídos y validados en `datos/`, la misma carga se ejecuta con
   `python fuentes/usgs_catalogo/lakehouse.py --inicio 2026-01 --fin 2026-09`.

Los datos viven en los volúmenes de Docker `minio-datos` e `iceberg-catalogo` y **sobreviven a
`docker compose --profile airflow down`**; solo `down -v` los borra. Para consultar sin prender
Airflow: `docker compose --profile lakehouse up -d`.

---

## 8. Data Lake en MinIO on-premise: trazas del CSN (NiFi → MinIO)

El perfil `nifi` también levanta un **MinIO propio del data lake** (aparte del MinIO del lakehouse)
y crea sus dos buckets. El flujo de NiFi escribe cada miniSEED en MinIO con el procesador
**PutS3Object**, además de la copia local en `datos/raw`.

```bash
docker compose --profile nifi up -d
python herramientas/nifi/crear_flujo.py --iniciar
```

Si el grupo de NiFi ya existía (de la sección 2, antes de esta fase), bórrenlo en NiFi y vuelvan a
ejecutar `crear_flujo.py` para que incluya el paso 9.

| Zona | Dónde queda | Qué es | Lo escribe |
|---|---|---|---|
| **Raw** | bucket `raw`: `csn_chile/red=C1/estacion=.../canal=.../anio=AAAA/dia_juliano=DDD/*.mseed` | El miniSEED binario original, **con versionado** (nunca se pierde una versión) | NiFi |
| **Curated** | bucket `curated`, misma ruta: `cabeceras.parquet` | Metadatos de cabecera de cada miniSEED (estación, canal, Hz, codificación, inicio/fin) | ObsPy |
| **Curated** | bucket `curated`, misma ruta: `ventanas.zarr` | Tensor `float32 [ventana, muestra]` en m/s o m/s²: ventanas de 60 s sin vacíos, alineadas al minuto | ObsPy + Zarr |

**Qué mostrar:**
1. En NiFi, el paso **"9. Data Lake MinIO (zona raw)"**: el mismo FlowFile sale de "7. Nombre del
   archivo" hacia el disco y hacia MinIO. En *View data provenance* se ve el evento `SEND` a
   `http://minio-datalake:9000`. Si MinIO no responde, NiFi reintenta 3 veces antes de enviarlo a Control.
2. Consola del data lake (http://localhost:9003, admin / sismologia2026) → bucket `raw` →
   la estructura jerárquica `red / estacion / canal / anio / dia_juliano` que pide el diseño, para
   leer en secuencia todo un canal o un día. En un objeto, *Metadata* muestra `estacion`, `canal`,
   `inicio-utc`, `fin-utc` y `ingestado-por: nifi`, y *Versions* muestra el versionado.
3. Curado (el "proceso secundario" del diseño): une los tramos de 5 minutos de cada día, pasa de
   cuentas a unidades físicas con el StationXML del CSN y corta las ventanas. Solo procesa las
   particiones con miniSEED nuevos, así que se puede ejecutar cada vez que quieran:
   ```bash
   python fuentes/csn_chile/datalake.py
   ```
4. Contenido del data lake y lectura de un lote como lo haría un `DataLoader` de PyTorch:
   ```bash
   python fuentes/csn_chile/consultar_datalake.py
   ```
5. Por qué un Data Lake y no una base de datos: son millones de muestras por canal (100–200 Hz).
   El objeto guarda el binario sin alterar la señal, y Zarr guarda el tensor en bloques que se
   descargan solo cuando se necesitan.

Los datos viven en el volumen de Docker `datalake-datos` y **sobreviven a
`docker compose --profile nifi down`**; solo `down -v` los borra (y no toca el lakehouse). Para
consultar sin prender NiFi: `docker compose --profile datalake up -d`.

---

## Recomendaciones para la presentación

- **Graben un video de cada demo** antes del día, como respaldo si falla internet o Docker.
- **Prendan antes lo lento:** NiFi (2 min), Airflow (2 min) y la primera Lambda.
- Si el profesor pregunta por qué los formatos "desaparecen": no desaparecen. El original siempre
  queda en `datos/raw` (capa bronce); el Parquet de `datos/silver` es la capa plata.
