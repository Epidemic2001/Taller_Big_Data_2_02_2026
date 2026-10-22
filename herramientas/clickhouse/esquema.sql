-- Data Warehouse de los testimonios EMSC LastQuake: modelo estrella en ClickHouse.
--
-- ClickHouse ejecuta este archivo cada vez que arranca (docker-entrypoint-initdb.d); todo es
-- IF NOT EXISTS, así que no borra nada. Lo carga el job de Spark (fuentes/emsc_lastquake/warehouse.py).
--
-- Las tablas son ReplacingMergeTree: si Spark vuelve a enviar un registro con la misma clave
-- (ORDER BY), ClickHouse se queda con una sola versión, la más reciente. Para leer sin duplicados
-- pendientes de fusionar se usa FINAL (lo hace la vista v_testimonios).

CREATE DATABASE IF NOT EXISTS sismologia;

-- El sismo según EMSC. Si EMSC corrige la magnitud, gana la carga más reciente.
CREATE TABLE IF NOT EXISTS sismologia.dim_evento
(
    unid            String,
    tiempo_utc      DateTime64(3, 'UTC'),
    latitud         Float64,
    longitud        Float64,
    profundidad_km  Float32,
    magnitud        Float32,
    tipo_magnitud   LowCardinality(String),
    clase_magnitud  LowCardinality(String),
    region          String,
    cargado_utc     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(cargado_utc)
ORDER BY unid;

-- Dónde se sintió: celda de 0,1° (~11 km) y su macrozona de Chile (aproximada por latitud).
CREATE TABLE IF NOT EXISTS sismologia.dim_ubicacion
(
    ubicacion_key   String,
    latitud_celda   Float32,
    longitud_celda  Float32,
    macrozona       LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY ubicacion_key;

-- Cuándo se reportó, en hora de Chile (America/Santiago). Clave: AAAAMMDDHH.
CREATE TABLE IF NOT EXISTS sismologia.dim_tiempo
(
    tiempo_key        UInt32,
    fecha             Date,
    hora_local        UInt8,
    dia_semana        LowCardinality(String),
    es_fin_de_semana  UInt8,
    franja            LowCardinality(String),
    mes               UInt8,
    anio              UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY tiempo_key;

-- Un testimonio ciudadano. Columnar, ordenado por sismo y fecha, particionado por mes.
CREATE TABLE IF NOT EXISTS sismologia.fact_felt_reports
(
    report_id               Int64,                   -- hash del testimonio: siempre el mismo id
    unid                    String,                  -- -> dim_evento
    ubicacion_key           String,                  -- -> dim_ubicacion
    tiempo_key              UInt32,                  -- -> dim_tiempo
    tiempo_utc              DateTime64(3, 'UTC'),
    canal                   LowCardinality(String),  -- app / mob / tel / web
    intensidad_cruda        UInt8,
    intensidad_corregida    Float32,                 -- corregida según Bossu et al. (2016), escala EMS-98
    nivel_danio_estimado    LowCardinality(String),  -- según la definición de cada grado EMS-98
    distancia_epicentro_km  Nullable(Float32),
    minutos_desde_sismo     Nullable(Float32),
    procesado_utc           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(procesado_utc)
PARTITION BY toYYYYMM(tiempo_utc)
ORDER BY (unid, tiempo_utc, report_id);

-- El modelo estrella ya unido y sin duplicados, para consultas y herramientas de BI.
-- Una vista no guarda datos, así que se reemplaza en cada arranque para reflejar cambios.
CREATE OR REPLACE VIEW sismologia.v_testimonios AS
SELECT
    f.report_id, f.unid AS unid, f.tiempo_utc AS tiempo_utc, f.canal, f.intensidad_cruda, f.intensidad_corregida,
    f.nivel_danio_estimado, f.distancia_epicentro_km, f.minutos_desde_sismo,
    e.magnitud, e.clase_magnitud, e.profundidad_km, e.region,
    u.macrozona, u.latitud_celda, u.longitud_celda,
    t.fecha, t.hora_local, t.dia_semana, t.franja
FROM sismologia.fact_felt_reports AS f FINAL
LEFT JOIN (SELECT * FROM sismologia.dim_evento FINAL) AS e ON e.unid = f.unid
LEFT JOIN (SELECT * FROM sismologia.dim_ubicacion FINAL) AS u ON u.ubicacion_key = f.ubicacion_key
LEFT JOIN (SELECT * FROM sismologia.dim_tiempo FINAL) AS t ON t.tiempo_key = f.tiempo_key;
