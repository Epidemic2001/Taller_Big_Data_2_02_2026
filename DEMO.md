# Guía de demo: las 5 herramientas de ingesta

Todo se ejecuta desde la carpeta del proyecto, en la terminal Bash de VS Code y con el `.venv` activo:

```bash
cd ~/universidad/big-data-sismologia
source .venv/Scripts/activate
pip install -r requirements.txt        # una vez (agrega kafka-python y boto3)
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
   guardar el miniSEED.
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
1. El DAG **`usgs_catalogo_chile`** y su grafo: `extraer_quakeml → validar_calidad`.
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

## Recomendaciones para la presentación

- **Graben un video de cada demo** antes del día, como respaldo si falla internet o Docker.
- **Prendan antes lo lento:** NiFi (2 min), Airflow (2 min) y la primera Lambda.
- Si el profesor pregunta por qué los formatos "desaparecen": no desaparecen. El original siempre
  queda en `datos/raw` (capa bronce); el Parquet de `datos/silver` es la capa plata.
