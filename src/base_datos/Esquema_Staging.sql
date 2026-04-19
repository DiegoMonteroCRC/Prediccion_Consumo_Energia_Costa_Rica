CREATE SCHEMA IF NOT EXISTS "Staging";


CREATE TABLE IF NOT EXISTS "Staging".stg_clima_nasa (
    empresa                  VARCHAR(20),
    ano                      INTEGER,
    mes                      INTEGER,
    t2m                      DOUBLE PRECISION,
    ws10m                    DOUBLE PRECISION,
    cloud_amt                DOUBLE PRECISION,
    rh2m                     DOUBLE PRECISION,
    t2m_max                  DOUBLE PRECISION,
    t2m_min                  DOUBLE PRECISION,
    cloud_od                 DOUBLE PRECISION,
    gwetroot                 DOUBLE PRECISION,
    ts                       DOUBLE PRECISION,
    prectotcorr              DOUBLE PRECISION,
    allsky_sfc_sw_dwn        DOUBLE PRECISION,
    ps                       DOUBLE PRECISION,
    t2mwet                   DOUBLE PRECISION,
    allsky_sfc_sw_diff       DOUBLE PRECISION,
    allsky_sfc_lw_dwn        DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS "Staging".stg_aresep_medios (
    mes                      INTEGER,
    ano                      INTEGER,
    empresa                  VARCHAR(25),
    tarifa                   VARCHAR(50),
    abonados                 DOUBLE PRECISION,
    ventas                   DOUBLE PRECISION,
    ingreso_sin_cvg          DOUBLE PRECISION,
    ingreso_con_cvg          DOUBLE PRECISION,
    precio_medio_sin_cvg     DOUBLE PRECISION,
    precio_medio_con_cvg     DOUBLE PRECISION,
    trimestre                VARCHAR(20),
    sistema                  VARCHAR(20),
    trimestral               VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS "Staging".stg_centro (
    stg_centro_key           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_objecto               INTEGER,
    operador                 VARCHAR(100),
    central_electrica        VARCHAR(100),
    fuente                   VARCHAR(50),
    provincia                VARCHAR(50),
    canton                   VARCHAR(50),
    distrito                 VARCHAR(50),
    codigo_dta               INTEGER,
    coordenada_x             DOUBLE PRECISION,
    coordenada_y             DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS "Staging".stg_zonas (
    stg_zona_key             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_objecto               INTEGER UNIQUE,
    operador                 VARCHAR(100),
    descripcion              VARCHAR(200),
    area                     NUMERIC(18,8),
    coordenadas              TEXT,
    tipo_geometria           VARCHAR(20),
    srid                     INTEGER NOT NULL DEFAULT 5367,
    fecha_carga              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "Staging".stg_distribucion (
    stg_distribucion_key     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_mes                   INTEGER,
    mes                      VARCHAR(15),
    anho                     INTEGER,
    empresa                  VARCHAR(25),
    tipo_tarifa              VARCHAR(20),
    descripcion_tarifa       VARCHAR(120),
    bloque                   VARCHAR(120),
    tarifa_promedio          DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS "Staging".stg_hidrocarburos (
    stg_hidrocarburos_key            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numero_expediente                VARCHAR(50),
    numero_resolucion                VARCHAR(50),
    fecha_publicacion                DATE,
    alcance_gaceta                   VARCHAR(20),
    numero_gaceta                    INTEGER,
    producto                         VARCHAR(100),
    tipo_cambio                      DOUBLE PRECISION,
    precio_referencia_internacional  DOUBLE PRECISION,
    precio_colonizado                DOUBLE PRECISION,
    otros_ingresos_prorrateados      DOUBLE PRECISION,
    asig_cruz_pesc                   DOUBLE PRECISION,
    asig_cruz_minae                  DOUBLE PRECISION,
    sub_cruz_pesc                    DOUBLE PRECISION,
    sub_cruz_minae                   DOUBLE PRECISION,
    diferencial_precios              DOUBLE PRECISION,
    impuesto_unico                   DOUBLE PRECISION,
    canon                            DOUBLE PRECISION,
    margen_operacion                 DOUBLE PRECISION,
    rend_tarif                       DOUBLE PRECISION,
    precio_plantel_sin_impuesto      DOUBLE PRECISION,
    precio_con_impuesto              DOUBLE PRECISION,
    margen_estaciones_terrestres     DOUBLE PRECISION,
    margen_estaciones_aereas         DOUBLE PRECISION,
    flete_estaciones                 DOUBLE PRECISION,
    margen_envasador                 DOUBLE PRECISION,
    margen_distribuidor              DOUBLE PRECISION,
    margen_detallista                DOUBLE PRECISION,
    rige                             BOOLEAN,
    precio_consumidor_final_gas      DOUBLE PRECISION,
    precio_final                     DOUBLE PRECISION,
    precio_final_sin_punto_fijo      DOUBLE PRECISION
);
