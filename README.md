# Arquitectura de Datos Sismológicos

Este proyecto implementa un *pipeline* de Big Data para la ingesta, procesamiento y almacenamiento de datos sismológicos. La arquitectura integra catálogos históricos (procesamiento *batch*), flujos de telemetría y alertas tempranas en tiempo real (*streaming*), trazas binarias de alta frecuencia, reportes ciudadanos y grillas geoespaciales, obtenidos de las principales redes sismológicas globales y de Chile.

## 📡 Fuentes de datos y tecnologías de ingesta

| Fuente de datos | Propósito en el proyecto | Método y formato | Stack tecnológico |
| --- | --- | --- | --- |
| **USGS** (EE.UU.) | Data Lake histórico y entrenamiento de modelos. | API REST (FDSN) con consultas particionadas por fecha para no superar el límite de 20.000 resultados por consulta.<br>**Formato:** XML (QuakeML) | Python (`requests`) + Apache Airflow |
| **EMSC** (Europa/Global) | Alertas tempranas en tiempo real (EEW). | Conexión asíncrona persistente (*streaming* puro).<br>**Formato:** JSON sobre WebSocket | Python (`websockets`) + Apache Kafka |
| **CSN** (Chile) | Datos crudos para *Deep Learning* (acelerómetros y GNSS). | Protocolo SeedLink para consumir flujos de trazas físicas.<br>**Formato:** `miniSEED` (binario) | ObsPy (Python) + Apache NiFi |
| **EMSC LastQuake** | Cruce de métricas con reportes de sismología ciudadana (*felt reports*). | API REST consultada por ID de evento para obtener testimonios ciudadanos de daños físicos.<br>**Formato:** JSON | Python (`requests`) + Apache Spark |
| **USGS** (ShakeMap) | Mapa de datos de sismos calibrado en la zona de Calama. | Formato de Raster, representando una cuadrícula de celdas en bits de información visual o espacial.<br>**Formato:** GeoTIFF / Grid XML | Python / AWS Lambda + GDAL |

## 🗄️ Arquitectura de almacenamiento por tipo de ingesta

Para responder a la naturaleza heterogénea de las 5 ingestas (datos estructurados, semi-estructurados, binarios de series temporales y matrices geoespaciales), la capa de persistencia implementa un enfoque de **persistencia políglota** que combina **Data Lake, Data Lakehouse, Data Warehouse, Bases de Datos NoSQL y Bases de Datos Relacionales/Geoespaciales**:

| Fuente de datos | Paradigma de almacenamiento | Motor tecnológico sugerido | Formato de persistencia y esquema | Estrategia de particionamiento e indexación |
| --- | --- | --- | --- | --- |
| **USGS** (Catálogo FDSN) | **Data Lakehouse** | **Apache Iceberg** o **Delta Lake** sobre MinIO / AWS S3 | **Bronze:** XML crudo (`QuakeML`) comprimido (`.xml.gz`).<br>**Silver/Gold:** Tablas abiertas en **Apache Parquet** con soporte ACID y *Time Travel*. | Particionado físico por `year=YYYY/month=MM` y *clustering* por región/magnitud para alimentar entrenamientos de ML reproducibles. |
| **EMSC** (Alertas EEW) | **Base de Datos NoSQL** (Documental + Series de Tiempo / Caché) | **MongoDB** o **Apache Cassandra** (+ **Redis** para estado activo) | Documentos **JSON / BSON** con esquema flexible para actualizaciones evolutivas del evento sísmico en curso. | Índice compuesto por `event_id` y `timestamp` descendente, índice geoespacial `2dsphere` sobre el epicentro y política `TTL` en caché para alertas vigentes. |
| **CSN** (Trazas Acelerómetros y GNSS) | **Data Lake** (Almacenamiento de Objetos) | **MinIO** (On-Premise) o **Amazon S3** (Cloud) | **Raw:** Binario nativo `miniSEED` (`.mseed`).<br>**Curated (DL):** Tensores multidimensionales en **HDF5 / Zarr** o **TFRecord**. | Estructura jerárquica de prefijos: `network/station/channel/year/julian_day/` para lectura secuencial masiva desde PyTorch o TensorFlow. |
| **EMSC LastQuake** (*Felt Reports*) | **Data Warehouse** (OLAP Columnar) | **ClickHouse**, **Google BigQuery** o **Amazon Redshift** | Modelo dimensional (**Esquema Estrella**): tabla de hechos `fact_felt_reports` y dimensiones `dim_evento`, `dim_ubicacion`, `dim_tiempo`. | Almacenamiento columnar ordenado por `event_id` y fecha, optimizado para agregaciones masivas y cruce con parámetros instrumentales en BI. |
| **USGS ShakeMap** (Calama) | **Base de Datos Relacional Geoespacial** (+ Object Storage) | **PostgreSQL + PostGIS** (`postgis_raster`) + S3 para archivos pesados | Tablas relacionales con columnas `RASTER` y `GEOMETRY`, respaldadas por archivos **COG (Cloud Optimized GeoTIFF)**. | Índice espacial **R-Tree (GiST)** sobre la envolvente de las celdas (`ST_ConvexHull(rast)`) y clave foránea relacional hacia el catálogo de eventos (`event_id`). |

## 🔍 Detalle técnico de cada almacenamiento

### 1. USGS (Catálogo Histórico) → Data Lakehouse (Delta Lake / Apache Iceberg)

* **Rol en la arquitectura:** Combina la flexibilidad del Data Lake con la gobernanza y transacciones ACID de un Data Warehouse.
* **Flujo de persistencia:** Apache Airflow deposita inicialmente los lotes de archivos `XML (QuakeML)` en la zona *Bronze* del almacenamiento de objetos. Posteriormente, un trabajo de transformación convierte las etiquetas XML en tablas **Parquet** gestionadas con **Delta Lake** o **Apache Iceberg** (zonas *Silver* y *Gold*).
* **Ventaja clave:** Permite versionado de datos (*Time Travel*) para auditar con qué corte exacto del catálogo histórico se entrenó cada versión de los modelos predictivos, evitando lecturas lentas sobre archivos XML anidados.

### 2. EMSC (Alertas Tempranas EEW) → Base de Datos NoSQL (MongoDB / Cassandra + Redis)

* **Rol en la arquitectura:** Ingesta de ultra-baja latencia para eventos sísmicos en desarrollo que emiten múltiples actualizaciones por segundo.
* **Flujo de persistencia:** El consumidor de Apache Kafka / Apache Flink procesa el flujo JSON proveniente del WebSocket y escribe directamente en una colección NoSQL (**MongoDB** o **Cassandra**), mientras actualiza en **Redis** el estado del último sismo activo.
* **Ventaja clave:** Los eventos de alerta temprana cambian de magnitud, profundidad y localización conforme más estaciones reportan datos. El modelo documental NoSQL absorbe escrituras concurrentes sin bloqueos de esquema y conserva el historial de revisiones de cada alerta en tiempo real.

### 3. CSN Chile (Formas de Onda Crudas) → Data Lake (MinIO / Amazon S3)

* **Rol en la arquitectura:** Repositorio inmutable de alto volumen para archivos binarios crudos y matrices de entrenamiento de *Deep Learning*.
* **Flujo de persistencia:** Apache NiFi y ObsPy reciben los paquetes `miniSEED` vía SeedLink, extraen los metadatos de cabecera (estación, canal, frecuencia de muestreo, ventana temporal) y escriben los bloques binarios directamente en *buckets* de **MinIO / S3**. Un proceso secundario empaqueta ventanas de eventos en formatos **Zarr** o **HDF5** listos para GPU.
* **Ventaja clave:** Las bases de datos relacionales o documentales se degradan al almacenar arreglos continuos de millones de muestras por segundo (100 Hz a 250 Hz por canal). El Data Lake ofrece almacenamiento de objetos escalable y económico sin alterar la fidelidad física de la señal.

### 4. EMSC LastQuake (Sismología Ciudadana) → Data Warehouse (ClickHouse / BigQuery / Redshift)

* **Rol en la arquitectura:** Motor analítico OLAP para cruzar la percepción ciudadana (intensidad Mercalli modificada, testimonios y daños reportados) con las métricas físicas de los sismos.
* **Flujo de persistencia:** Apache Spark consume los archivos JSON de la API REST por `event_id`, limpia valores atípicos, geocodifica las coordenadas de los usuarios y carga los resultados en un **Data Warehouse** estructurado bajo un modelo estrella:
  * **Tabla de Hechos (`fact_felt_reports`):** `report_id`, `event_id`, `location_key`, `time_key`, `felt_intensity_mmi`, `damage_level`, `distance_to_epicenter_km`.
  * **Tablas de Dimensiones:** `dim_seismic_event` (magnitud, profundidad, agencia), `dim_geography` (país, región, comuna, tipo de suelo), `dim_time` (fecha, hora local, día de la semana).
* **Ventaja clave:** Acelera consultas analíticas complejas (por ejemplo, comparar la atenuación de intensidad percibida frente a la magnitud instrumental según la distancia epicentral) mediante compresión columnar y ejecución vectorizada.

### 5. USGS ShakeMap (Zona de Calama) → Base Relacional Geoespacial (PostgreSQL + PostGIS)

* **Rol en la arquitectura:** Almacenamiento relacional y espacial para consultar mapas calibrados de movimiento fuerte del suelo (PGA, PGV e intensidad instrumental) en el norte de Chile.
* **Flujo de persistencia:** La función AWS Lambda con **GDAL** procesa el archivo raster de ShakeMap, lo reproyecta al sistema de coordenadas local (SIRGAS-Chile / UTM Zona 19S) y genera dos salidas:
  1. Un archivo **COG (Cloud Optimized GeoTIFF)** en S3 para visualización rápida en mapas web.
  2. Teselas raster y metadatos relacionales insertados en **PostgreSQL + PostGIS** mediante `raster2pgsql`, vinculados por clave foránea (`event_id`) con el catálogo sísmico y tablas de infraestructura crítica de Calama (hospitales, subestaciones eléctricas, faenas mineras).
* **Ventaja clave:** Garantiza integridad referencial (ACID) y habilita funciones espaciales nativas en SQL (como `ST_Value`, `ST_Clip` y `ST_Intersects`) para determinar en milisegundos qué infraestructura o población de Calama se encuentra dentro de polígonos de intensidad severa.

## ⚙️ Flujo general de extremo a extremo

1. **Perímetro de Ingesta:** Los contenedores productores extraen los datos desde las 5 fuentes utilizando sus protocolos nativos (`REST XML/JSON`, `WebSocket`, `SeedLink` y ejecución *serverless* con `GDAL`).
2. **Desacoplamiento y Procesamiento:**
   * **Vía *Streaming* (Tiempo Real):** Los flujos continuos (`EMSC WebSocket` y metadatos de trazas `CSN`) se publican en tópicos de **Apache Kafka** y se procesan al vuelo con **Apache Flink** y **Apache NiFi**.
   * **Vía *Batch / Micro-batch*:** Los catálogos históricos (`USGS`), los reportes ciudadanos (`LastQuake`) y las grillas espaciales (`ShakeMap`) son orquestados y transformados mediante **Apache Airflow**, **Apache Spark** y **AWS Lambda**.
3. **Capa de Almacenamiento Políglota:**
   * Los binarios crudos (`miniSEED`) aterrizan directamente en el **Data Lake (S3 / MinIO)**.
   * El catálogo histórico (`XML`) se consolida en formato Parquet sobre el **Data Lakehouse (Delta Lake / Iceberg)**.
   * Las alertas tempranas (`JSON`) se indexan en la **Base NoSQL (MongoDB / Cassandra + Redis)** para consumo en vivo.
   * Los reportes ciudadanos procesados por Spark alimentan el esquema estrella en el **Data Warehouse (ClickHouse / BigQuery)**.
   * Los mapas de intensidad de Calama (`Raster`) se indexan espacialmente en la **Base Relacional Geoespacial (PostgreSQL + PostGIS)**.

## 📊 Consumo de datos

Las cinco formas en que se consume el ecosistema. Cada consumo lee directamente de uno de los almacenamientos descritos arriba, sin copiar los datos.

### Cinco formas de consumir nuestro ecosistema

![Tabla con los cinco tipos de consumo: alertamiento con un bot en Python sobre MongoDB, dashboards en Grafana sobre ClickHouse, API REST y mapa web Leaflet sobre PostGIS y S3, Machine Learning con FastAPI y scikit-learn sobre ClickHouse, y exploración ad-hoc en JupyterLab sobre el Lakehouse Iceberg y el Data Lake MinIO](docs/consumo/01_cinco_formas_de_consumo.png)

Son 5 de las 10 formas de consumo que lista la pauta (sección VI), una por tipo de uso: alerta en tiempo real, BI, aplicación con API, modelo de ML y exploración ad-hoc. Entre todas leen los 5 almacenamientos del ecosistema: MongoDB, ClickHouse, PostGIS + S3, el Lakehouse Iceberg y el Data Lake de MinIO. El dashboard y el modelo de ML comparten ClickHouse porque ahí están los testimonios con distancia y magnitud.

### Cómo se materializa: de la fuente al usuario

![Diagrama de flujo de cada fuente a su ingesta, su almacenamiento y su consumo: EMSC Streaming a Kafka, MongoDB y alertas; USGS Catálogo a Airflow, Iceberg y JupyterLab; CSN Chile a NiFi, MinIO y JupyterLab; EMSC LastQuake a Spark Streaming, ClickHouse, Grafana y el endpoint de ML; USGS ShakeMap a AWS Lambda, PostGIS + S3 y la API REST con mapa web](docs/consumo/02_de_la_fuente_al_usuario.png)

Cada fila es una fuente con su herramienta de ingesta y su almacenamiento, y el color indica qué consumo lee ese almacenamiento. USGS Catálogo y CSN Chile comparten JupyterLab; los testimonios de ClickHouse alimentan tanto el dashboard como el modelo de ML. Los servicios de consumo se conectan directamente a los almacenamientos que ya levanta Docker Compose.

### Un sismo, cinco consumos

![Línea de tiempo de los cinco consumos según la velocidad del dato: tiempo real (alertas), casi real (Grafana), por evento (API REST y mapa web), bajo demanda (endpoint de ML) e histórico (notebooks en JupyterLab), con las preguntas que responde cada uno y el almacenamiento que lee](docs/consumo/03_un_sismo_cinco_consumos.png)

El consumo sigue el tipo de ingesta: lo que llega por *streaming* se consume como alerta en segundos; los testimonios por *micro-batch* alimentan el dashboard; el ShakeMap por evento alimenta la API y el mapa; el modelo de ML responde bajo demanda; y el histórico por *batch* y los archivos del CSN se exploran en notebooks. Así, cada almacenamiento tiene un consumo coherente con su dato.

### 🚨 Implementación del consumo 1: alertas sísmicas en tiempo real

Bot en `consumo/alertas/alertas.py`: lee de **MongoDB** la colección `emsc_eventos`, que el consumidor de Kafka llena con el *streaming* de EMSC, y **avisa en segundos** cuando un sismo cumple dos reglas: **magnitud mínima** (M ≥ 4,5 por defecto) y **epicentro cerca de una ciudad vigilada** (a ≤ 300 km de una de 12 ciudades, de Arica a Punta Arenas). La alerta sale por la terminal y por correo, y también por **Slack** y **Telegram** si se configuran. Los correos llegan a **Mailpit**, una bandeja local que no envía correos reales, para demostrar el canal sin cuentas externas.

```bash
docker compose --profile kafka --profile alertas up -d   # Kafka + MongoDB + Mailpit
python fuentes/emsc_streaming/productor_kafka.py --kafka localhost:9092 --duracion 900   # terminal 1
python fuentes/emsc_streaming/consumidor_mongodb.py --kafka localhost:9092 --continuo     # terminal 2
python consumo/alertas/alertas.py --continuo                                              # terminal 3: el bot
```

Bandeja de correo en <http://localhost:8025> y Mongo Express en <http://localhost:8082>. Los pasos completos de la demo están en [DEMO.md, sección 11](DEMO.md#11-consumo-alertas-sísmicas-en-tiempo-real-mongodb).

| Opción | Qué hace |
| --- | --- |
| `--continuo` | Revisa MongoDB cada 10 segundos (`--intervalo`) hasta que se detiene con Ctrl+C. Sin ella, revisa una vez y termina |
| `--umbral 5 --radio 200` | Cambia la magnitud mínima y el radio en km alrededor de cada ciudad |
| `--ciudades Santiago Calama` | Vigila solo esas ciudades (por defecto, las 12) |
| `--horas 24` | Considera solo los sismos de las últimas N horas |
| `--probar` | Envía una alerta de prueba con el último sismo guardado |
| `--demo` | Demo en vivo: sin filtros (M ≥ 0, todo el mundo), revisa cada 2 segundos y avisa solo los sismos que lleguen desde ese momento |
| `--sin-correo` | Avisa solo por la terminal, Slack y Telegram |

**Cómo decide:** MongoDB filtra por magnitud, por la ventana de tiempo y por un círculo `$geoWithin` / `$centerSphere` alrededor de cada ciudad, que usa el índice geoespacial `2dsphere` de `epicentro`. Como ese círculo es aproximado, el bot confirma la distancia con la fórmula de haversine. Cada alerta queda en la colección `alertas_enviadas`, así que un sismo se avisa una sola vez aunque el bot se reinicie; si EMSC corrige la magnitud en 0,5 o más, llega una **ACTUALIZACIÓN**. El mensaje trae el nivel (moderado bajo M5, fuerte desde M5 y mayor desde M6), la hora de Chile, la ciudad más cercana y las tres siguientes, la profundidad y un enlace al mapa. Un canal caído no impide los demás. Slack y Telegram se activan con variables en `consumo/alertas/.env` (la plantilla es `consumo/alertas/.env.example`).

### 📈 Implementación del consumo 2: dashboard de BI en Grafana

Dashboard en `consumo/dashboard/grafana/`: un **Grafana** con el plugin oficial de ClickHouse que consulta con SQL la vista `v_testimonios` del **Data Warehouse de ClickHouse**, el modelo estrella que Spark carga con los testimonios ciudadanos de LastQuake. Muestra dónde, cuándo y cómo se sienten los sismos. El datasource y el dashboard "Sismos sentidos en Chile" se cargan solos al levantar el contenedor (*provisioning*), así que no hay que configurar nada a mano.

```bash
docker compose --profile spark --profile dashboard up -d --build   # Spark + ClickHouse + Grafana
python fuentes/emsc_lastquake/sondeo.py --desde 2026-09-01 --ciclos 1   # testimonios para los paneles
```

Dashboard en <http://localhost:3000>: se ve sin iniciar sesión, y para editarlo el usuario es admin / sismologia. Si ClickHouse ya tiene testimonios, basta con `docker compose --profile dashboard up -d`. Los pasos completos de la demo están en [DEMO.md, sección 12](DEMO.md#12-consumo-dashboard-de-bi-en-grafana-clickhouse).

| Panel | Qué muestra |
| --- | --- |
| Indicadores | Testimonios, sismos con testimonios, intensidad promedio (EMS-98) y distancia mediana al epicentro |
| Testimonios por macrozona | Dónde se sintieron los sismos |
| Atenuación | Intensidad promedio en tramos de 25 km de distancia al epicentro: cae a medida que la distancia aumenta |
| Testimonios por hora | Cada pico es un sismo: los reportes llegan en los minutos siguientes |
| Canal de reporte | App móvil, web móvil, web o teléfono |
| Daño estimado (EMS-98) | Testimonios por nivel de daño, según los cortes que aplica Spark al cargar |
| Sismos con más testimonios | Región, magnitud, profundidad, intensidad promedio y máxima, y hasta qué distancia se sintió cada sismo |

**Cómo se consulta:** Grafana se conecta a ClickHouse por el protocolo nativo dentro de la red de Docker (`clickhouse:9000`). Cada panel es una consulta SQL sobre `sismologia.v_testimonios` filtrada con `$__timeFilter(tiempo_utc)`, así que al cambiar el rango de tiempo (90 días por defecto) ClickHouse recalcula todos los paneles. El dashboard se refresca cada minuto, al mismo ritmo que los *micro-batches* de Spark, y como la vista usa `FINAL`, un testimonio que Spark reenvía no se cuenta dos veces. La conexión está en `provisioning/datasources/clickhouse.yml` y el dashboard en `dashboards/sismologia.json`, ambos dentro de `consumo/dashboard/grafana/`.

### 🗺️ Implementación del consumo 3: daños y rutas de emergencia en Calama

Aplicación web en `consumo/api_mapa/`: una API **FastAPI** y un **mapa web en Leaflet** que leen directamente de PostgreSQL + PostGIS (y el COG de S3). Para cada sismo con ShakeMap muestra qué **daño potencial** recibió cada sector de Calama y cada instalación crítica. También calcula **rutas de emergencia** sobre la red vial de OpenStreetMap que evitan las calles de mayor intensidad, incluida la ruta al hospital operativo más cercano.

```bash
docker compose --profile lambda --profile mapa up -d --build
python consumo/api_mapa/cargar_calama.py      # una vez: red vial y sectores de Calama (OpenStreetMap)
```

Mapa en <http://localhost:8090> y documentación interactiva de la API en <http://localhost:8090/docs>. Los pasos completos de la demo están en [DEMO.md, sección 13](DEMO.md#13-consumo-api-rest--mapa-web-daños-y-rutas-de-emergencia-postgis--s3).

| Endpoint | Qué entrega |
| --- | --- |
| `GET /api/eventos` | Sismos con ShakeMap, ordenados por la intensidad que recibió Calama |
| `GET /api/eventos/{id}/sectores` | Sectores de Calama con su MMI promedio y máxima y su daño potencial (GeoJSON, `ST_Clip`) |
| `GET /api/eventos/{id}/infraestructura` | Hospitales, clínicas, subestaciones y faenas con su MMI, PGA y PGV (GeoJSON, `ST_Value`) |
| `GET /api/eventos/{id}/isosistas` | Polígonos de intensidad MMI del ShakeMap (GeoJSON) |
| `GET /api/eventos/{id}/resumen` | Sectores, instalaciones y km de calles por nivel de daño |
| `GET /api/eventos/{id}/red-vial?evitar_mmi=7` | Calles con daño potencial por grado MMI y calles bloqueadas (GeoJSON) |
| `GET /api/eventos/{id}/ruta?desde=lat,lon&hasta=lat,lon&evitar_mmi=7` | Ruta segura y ruta directa, con distancia, tiempo y exposición a la intensidad (GeoJSON) |
| `GET /api/eventos/{id}/ruta-hospital?desde=lat,lon&evitar_mmi=7` | Ruta segura al hospital o clínica operativo más cercano (GeoJSON) |
| `GET /api/eventos/{id}/cog` | ShakeMap completo (Cloud Optimized GeoTIFF de 3 bandas) desde S3 |

La **ruta segura** usa Dijkstra sobre el grafo de la red vial. Cada tramo cuesta su tiempo de viaje multiplicado por un castigo según la intensidad MMI que recibió: 5 veces con MMI VI y 10 veces con MMI VII. Los tramos sobre el umbral `evitar_mmi` quedan bloqueados (cuestan 1000 veces más), así que la ruta solo los usa si no hay otra forma de llegar.

### 🤖 Implementación del consumo 4: endpoint de Machine Learning

Servicio en `consumo/api_ml/`: una API **FastAPI** con un modelo de **scikit-learn** entrenado con los testimonios ciudadanos que Spark carga en el **Data Warehouse de ClickHouse** (vista `v_testimonios`). Recibe la magnitud, la profundidad y la distancia al epicentro, y responde la **intensidad EMS-98 esperada**, su grado, el **nivel de daño** y su incertidumbre. Sirve para simular escenarios de planes de emergencia comunales y de diseño de obras. Incluye un simulador web con la curva de atenuación y la intensidad esperada en cada capital regional.

```bash
docker compose --profile spark --profile ml up -d --build   # Spark + ClickHouse + el endpoint de ML
python fuentes/emsc_lastquake/sondeo.py --desde 2026-01-01 --ciclos 1   # testimonios para entrenar
```

Simulador en <http://localhost:8091> y documentación interactiva de la API en <http://localhost:8091/docs>. Los pasos completos de la demo están en [DEMO.md, sección 14](DEMO.md#14-consumo-endpoint-de-machine-learning-clickhouse--scikit-learn).

| Endpoint | Qué entrega |
| --- | --- |
| `GET /api/predecir?magnitud=7&profundidad_km=35&distancia_km=60` | Intensidad EMS-98 esperada, grado, nivel de daño, intervalo del 80 % y probabilidad de cada nivel de daño |
| `POST /api/predecir` | Lo mismo para hasta 1.000 escenarios en una llamada (por ejemplo, todos los de un plan de emergencia) |
| `GET /api/curva-atenuacion?magnitud=7&profundidad_km=35` | Cómo baja la intensidad esperada con la distancia |
| `GET /api/ciudades?magnitud=7&profundidad_km=35&latitud=-23.6&longitud=-70.9` | Intensidad y daño esperados en cada capital regional y en Calama para un epicentro |
| `GET /api/modelo` · `GET /api/modelo/versiones` | Modelo vigente (algoritmo, corte de datos, rango de entrenamiento y métricas) y todas sus versiones |
| `GET /api/modelo/evaluacion` | Monitoreo: error del modelo con los testimonios llegados después de su corte y si conviene reentrenar |
| `POST /api/modelo/reentrenar` | Entrena una versión nueva con lo que hoy tiene ClickHouse y la deja vigente |

**Cómo se entrena:** se comparan dos modelos con validación cruzada **agrupada por sismo** (`GroupKFold`). Así, los testimonios de un mismo sismo nunca quedan a la vez en entrenamiento y en prueba, y el error medido es el de un sismo que el modelo no vio. Los dos candidatos son:

- una **ecuación de atenuación** clásica, `I = c0 + c1·M + c2·ln(R) + c3·R` (con R la distancia hipocentral), cuyos signos están restringidos para que sea físicamente consistente;
- un **gradient boosting** (`HistGradientBoostingRegressor`) con restricciones monótonas: la intensidad solo puede subir con la magnitud y bajar con la distancia.

Gana el de menor error absoluto medio (MAE), y ambos se comparan contra un modelo que predice siempre el promedio. Cada entrenamiento queda como una **versión** en `datos/modelos/intensidad_ems98/`, con su corte de datos y sus métricas. Si un escenario sale del rango de magnitudes, profundidades o distancias vistas al entrenar, responde la ecuación de atenuación (los árboles no extrapolan) y la respuesta lo avisa.

Con los testimonios de 2026 (unos 2.400 de 325 sismos, M2,5–6,9) gana la ecuación de atenuación, con un error medio de 1 grado y un 86 % de aciertos a ±1 grado. Como aún no hay sismos grandes en los datos, subestima los escenarios M7,5+, que vienen marcados como fuera de rango; el detalle está en las limitaciones del DEMO.

### 📓 Implementación del consumo 5: consultas ad-hoc en JupyterLab

Servicio en `consumo/jupyterlab/`: un **JupyterLab** con **PyIceberg**, **DuckDB** y **Zarr** que lee directo del **Lakehouse Iceberg** del catálogo USGS y del **Data Lake MinIO** del CSN, sin copiar datos. Son tres notebooks para la exploración y los informes que no necesitan una aplicación propia: la evolución de la sismicidad, el *Time Travel* entre versiones del catálogo, la señal de un sismo en cada estación y la decisión de cuándo reentrenar el modelo de ML.

```bash
docker compose --profile jupyter up -d --build   # JupyterLab + MinIO y catálogo Iceberg + MinIO del data lake
```

JupyterLab en <http://localhost:8888> (token `sismologia`). Los pasos completos de la demo están en [DEMO.md, sección 15](DEMO.md#15-consumo-notebooks-en-jupyterlab-lakehouse-iceberg--data-lake-minio).

| Notebook | Lee | Qué responde |
| --- | --- | --- |
| `01_sismicidad_catalogo_usgs.ipynb` | Lakehouse: `gold.usgs_resumen_mensual` y `silver.usgs_sismos` | Sismos por mes y por macrozona, mapa y perfil de profundidad (la subducción de Nazca), ley de Gutenberg-Richter, lectura selectiva por particiones ocultas y *Time Travel* entre dos versiones del catálogo. Exporta la tabla del informe con el id del snapshot usado |
| `02_senales_csn_datalake.ipynb` | Data Lake: buckets `raw` y `curated` (y el catálogo USGS para cruzar) | Inventario de miniSEED, cabeceras y continuidad con SQL directo sobre MinIO, lectura de un lote Zarr para Deep Learning, detección de sismos por coincidencia entre estaciones confirmada con el catálogo, y la señal y el espectrograma de un sismo |
| `03_reentrenamiento_modelo_ml.ipynb` | Lakehouse y las versiones del modelo del consumo 4 (y su API, si está levantada) | Qué versión del catálogo existía cuando se entrenó cada modelo, qué sismos llegaron después, qué parte de la sismicidad cubre el dominio del modelo y si conviene reentrenar |

**Cómo se consulta:** PyIceberg pregunta al catálogo REST qué archivos Parquet forman la versión pedida de la tabla y entrega el resultado como Arrow, que DuckDB consulta con SQL en memoria (`scan().to_duckdb()`). En el Data Lake, DuckDB lee los `cabeceras.parquet` directo desde `s3://curated` con su extensión `httpfs` (las carpetas `anio=` y `dia_juliano=` pasan a ser columnas), y Zarr descarga del tensor solo los bloques de 32 ventanas que se piden, como lo haría un `DataLoader`. Las conexiones están en `consumo/jupyterlab/notebooks/conexiones.py`: en Docker usan los nombres de los servicios y, si no están esas variables de entorno, `localhost`, así que los notebooks también se abren desde Windows con el `.venv`.

Con los datos de 2026 (653 sismos del catálogo hasta el 3 de noviembre y la señal del CSN del 26 de octubre): el Norte Grande concentra el 60 % de los sismos y casi todos son de profundidad intermedia (solo un 15 % a menos de 70 km). Al recargar octubre completo, el *Time Travel* muestra los 24 sismos que faltaban en la versión anterior. En el data lake, la detección por coincidencia encuentra los tres sismos M4,6–4,9 de esa tarde y una cuarta señal que no está en el catálogo USGS (un sismo menor o ruido local), y en las estaciones a menos de 300 km la onda P llega donde se calcula. El modelo de ML cubre el 94 % de los sismos M ≥ 4 del catálogo; los que quedan fuera son más profundos que los 207 km de sus testimonios, y por ahora no hace falta reentrenarlo.
