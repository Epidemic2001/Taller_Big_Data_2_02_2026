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

### 🗺️ Implementación del consumo 3: daños y rutas de emergencia en Calama

Aplicación web en `consumo/api_mapa/`: una API **FastAPI** y un **mapa web en Leaflet** que leen directamente de PostgreSQL + PostGIS (y el COG de S3). Para cada sismo con ShakeMap muestra qué **daño potencial** recibió cada sector de Calama y cada instalación crítica. También calcula **rutas de emergencia** sobre la red vial de OpenStreetMap que evitan las calles de mayor intensidad, incluida la ruta al hospital operativo más cercano.

```bash
docker compose --profile lambda --profile mapa up -d --build
python consumo/api_mapa/cargar_calama.py      # una vez: red vial y sectores de Calama (OpenStreetMap)
```

Mapa en <http://localhost:8090> y documentación interactiva de la API en <http://localhost:8090/docs>. Los pasos completos de la demo están en [DEMO.md, sección 11](DEMO.md#11-consumo-api-rest--mapa-web-daños-y-rutas-de-emergencia-postgis--s3).

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
