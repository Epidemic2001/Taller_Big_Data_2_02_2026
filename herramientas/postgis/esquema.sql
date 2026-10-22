-- Base de datos relacional geoespacial del ShakeMap (USGS) en PostgreSQL + PostGIS.
--
-- PostgreSQL ejecuta este archivo al crear la base (docker-entrypoint-initdb.d); todo es
-- IF NOT EXISTS / OR REPLACE, así que también se puede volver a ejecutar sin perder datos.
-- La carga la Lambda shakemap-almacenar (herramientas/lambda/lambda_almacenamiento.py).
--
-- Rasters y geometrías están en EPSG:5361, SIRGAS-Chile 2002 / UTM zona 19S (metros): las
-- distancias y áreas salen directamente en metros.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
-- ST_FromGDALRaster convierte el COG (GeoTIFF) que envía la Lambda en un raster de PostGIS
ALTER DATABASE sismologia SET postgis.gdal_enabled_drivers TO 'GTiff';

-- Catálogo de sismos. event_id es el id del USGS: el mismo de silver.usgs_sismos en el lakehouse.
CREATE TABLE IF NOT EXISTS eventos (
    event_id        text PRIMARY KEY,
    magnitud        real,
    lugar           text,
    tiempo_utc      timestamptz,
    profundidad_km  real,
    epicentro       geometry(Point, 5361)
);

-- Un ShakeMap por sismo: metadatos y ubicación de su archivo pesado (COG) en S3.
CREATE TABLE IF NOT EXISTS shakemaps (
    event_id          text PRIMARY KEY REFERENCES eventos (event_id) ON DELETE CASCADE,
    version_shakemap  text,
    cog_s3            text NOT NULL,
    cog_bytes         bigint,
    cog_sha256        text,
    resolucion_m      real,
    bandas            text[],
    mmi_max           real,
    pga_max_g         real,
    pgv_max_cm_s      real,
    procesado_utc     timestamptz NOT NULL DEFAULT now()
);

-- El raster del ShakeMap cortado en teselas de 100 x 100 celdas.
-- Bandas: 1 = MMI, 2 = PGA (g), 3 = PGV (cm/s).
CREATE TABLE IF NOT EXISTS shakemap_teselas (
    id        bigserial PRIMARY KEY,
    event_id  text NOT NULL REFERENCES shakemaps (event_id) ON DELETE CASCADE,
    rast      raster NOT NULL CHECK (ST_SRID(rast) = 5361)
);
-- Índice espacial R-Tree (GiST) sobre la envolvente de cada tesela
CREATE INDEX IF NOT EXISTS shakemap_teselas_gix ON shakemap_teselas USING gist (ST_ConvexHull(rast));
CREATE INDEX IF NOT EXISTS shakemap_teselas_evento ON shakemap_teselas (event_id);

-- Polígonos de intensidad: zona donde la MMI del ShakeMap está entre mmi y mmi + 1.
CREATE TABLE IF NOT EXISTS shakemap_isosistas (
    event_id  text NOT NULL REFERENCES shakemaps (event_id) ON DELETE CASCADE,
    mmi       smallint NOT NULL,
    geom      geometry(MultiPolygon, 5361) NOT NULL,
    PRIMARY KEY (event_id, mmi)
);
CREATE INDEX IF NOT EXISTS shakemap_isosistas_gix ON shakemap_isosistas USING gist (geom);

-- Infraestructura crítica de Calama desde OpenStreetMap (fuentes/usgs_shakemap/infraestructura_calama.py).
CREATE TABLE IF NOT EXISTS infraestructura_calama (
    osm_id  text PRIMARY KEY,
    tipo    text NOT NULL,
    nombre  text,
    geom    geometry(Point, 5361) NOT NULL
);
CREATE INDEX IF NOT EXISTS infraestructura_calama_gix ON infraestructura_calama USING gist (geom);

-- Intensidad que recibió cada instalación en cada sismo: ST_Value sobre el raster.
-- ST_Intersects(raster, geometría) usa el índice GiST de las teselas.
CREATE OR REPLACE VIEW v_exposicion_infraestructura AS
SELECT DISTINCT ON (s.event_id, i.osm_id)
       s.event_id, e.magnitud, e.lugar, i.tipo, i.nombre,
       round(ST_Value(t.rast, 1, i.geom)::numeric, 2) AS mmi,
       round(ST_Value(t.rast, 2, i.geom)::numeric, 3) AS pga_g,
       round(ST_Value(t.rast, 3, i.geom)::numeric, 1) AS pgv_cm_s,
       round((ST_Distance(i.geom, e.epicentro) / 1000)::numeric) AS distancia_epicentro_km
FROM infraestructura_calama AS i
JOIN shakemap_teselas AS t ON ST_Intersects(t.rast, i.geom)
JOIN shakemaps AS s ON s.event_id = t.event_id
JOIN eventos AS e ON e.event_id = s.event_id
ORDER BY s.event_id, i.osm_id;
