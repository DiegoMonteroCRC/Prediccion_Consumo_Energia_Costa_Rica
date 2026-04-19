-- ============================================
-- DW - ESQUEMA FACT_DIM
-- ============================================

CREATE SCHEMA IF NOT EXISTS "Fact_Dim";


-- ============================================
-- DIMENSION TIEMPO
-- Se usa tanto para granularidad mensual como diaria
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_tiempo (
    tiempo_key           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha                DATE NOT NULL UNIQUE,
    anio                 INTEGER NOT NULL,
    mes                  INTEGER NOT NULL,
    dia                  INTEGER NOT NULL,
    trimestre            INTEGER NOT NULL,
    nombre_mes           VARCHAR(20) NOT NULL,
    anio_mes             INTEGER NOT NULL
);

-- ============================================
-- DIMENSION EMPRESA
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_empresa (
    empresa_key          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre_empresa       VARCHAR(100) NOT NULL UNIQUE
);

-- ============================================
-- DIMENSION TARIFA
-- Unifica catálogos usados por aresep_unificado y distribucion
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_tarifa (
    tarifa_key               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tarifa_nk                VARCHAR(500) NOT NULL UNIQUE,
    nombre_tarifa            VARCHAR(100),
    tipo_tarifa             VARCHAR(50),
    descripcion_tarifa      VARCHAR(150),
    bloque                  VARCHAR(100),
    sistema                 VARCHAR(50),
    indicador_trimestral    VARCHAR(20)
);

-- ============================================
-- DIMENSION UBICACION
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_ubicacion (
    ubicacion_key         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ubicacion_nk          VARCHAR(500) NOT NULL UNIQUE,
    provincia             VARCHAR(100),
    canton                VARCHAR(100),
    distrito              VARCHAR(100),
    codigo_dta            INTEGER,
    coordenada_x          DOUBLE PRECISION,
    coordenada_y          DOUBLE PRECISION
);

-- ============================================
-- DIMENSION CENTRAL ELECTRICA
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_central_electrica (
    central_key           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_objecto            INTEGER NOT NULL UNIQUE,
    operador              VARCHAR(150),
    central_electrica     VARCHAR(150),
    fuente                VARCHAR(100),
    ubicacion_key         BIGINT REFERENCES "Fact_Dim".dim_ubicacion(ubicacion_key)
);

-- ============================================
-- DIMENSION PRODUCTO HIDROCARBURO
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_producto_hidrocarburo (
    producto_key          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    producto              VARCHAR(150) NOT NULL UNIQUE
);

-- ============================================
-- DIMENSION RESOLUCION HIDROCARBURO
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_resolucion_hidrocarburo (
    resolucion_key                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    resolucion_nk                     VARCHAR(300) NOT NULL UNIQUE,
    numero_expediente                 VARCHAR(50),
    numero_resolucion                 VARCHAR(50),
    fecha_publicacion                 DATE,
    alcance_gaceta                    VARCHAR(20),
    numero_gaceta                     INTEGER,
    rige                              BOOLEAN
);

-- ============================================
-- HECHO CLIMA MENSUAL
-- Grano: empresa + mes
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".fact_clima_mensual (
    fact_clima_key        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tiempo_key            BIGINT NOT NULL REFERENCES "Fact_Dim".dim_tiempo(tiempo_key),
    empresa_key           BIGINT NOT NULL REFERENCES "Fact_Dim".dim_empresa(empresa_key),

    t2m                   NUMERIC(18,6),
    ws10m                 NUMERIC(18,6),
    cloud_amt             NUMERIC(18,6),
    rh2m                  NUMERIC(18,6),
    t2m_max               NUMERIC(18,6),
    t2m_min               NUMERIC(18,6),
    cloud_od              NUMERIC(18,6),
    gwetroot              NUMERIC(18,6),
    ts                    NUMERIC(18,6),
    prectotcorr           NUMERIC(18,6),
    allsky_sfc_sw_dwn     NUMERIC(18,6),
    ps                    NUMERIC(18,6),
    t2mwet                NUMERIC(18,6),
    allsky_sfc_sw_diff    NUMERIC(18,6),
    allsky_sfc_lw_dwn     NUMERIC(18,6),

    UNIQUE (tiempo_key, empresa_key)
);

-- ============================================
-- HECHO TARIFA ELECTRICIDAD
-- Fuente: stg_aresep_unificado_2020_2025
-- Grano: empresa + mes + tarifa
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".fact_tarifa_electricidad (
    fact_tarifa_key          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tiempo_key               BIGINT NOT NULL REFERENCES "Fact_Dim".dim_tiempo(tiempo_key),
    empresa_key              BIGINT NOT NULL REFERENCES "Fact_Dim".dim_empresa(empresa_key),
    tarifa_key               BIGINT NOT NULL REFERENCES "Fact_Dim".dim_tarifa(tarifa_key),

    abonados                 NUMERIC(18,6),
    ventas                   NUMERIC(18,6),
    ingreso_sin_cvg          NUMERIC(18,6),
    ingreso_con_cvg          NUMERIC(18,6),
    precio_medio_sin_cvg     NUMERIC(18,6),
    precio_medio_con_cvg     NUMERIC(18,6),

    UNIQUE (tiempo_key, empresa_key, tarifa_key)
);

-- ============================================
-- HECHO DISTRIBUCION TARIFARIA
-- Fuente: stg_distribucion
-- Grano: empresa + mes + tarifa
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".fact_distribucion_tarifaria (
    fact_distribucion_key    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tiempo_key               BIGINT NOT NULL REFERENCES "Fact_Dim".dim_tiempo(tiempo_key),
    empresa_key              BIGINT NOT NULL REFERENCES "Fact_Dim".dim_empresa(empresa_key),
    tarifa_key               BIGINT NOT NULL REFERENCES "Fact_Dim".dim_tarifa(tarifa_key),

    tarifa_promedio          NUMERIC(18,6),

    UNIQUE (tiempo_key, empresa_key, tarifa_key)
);

-- ============================================
-- HECHO HIDROCARBUROS
-- Grano: fecha_publicacion + producto + resolucion
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".fact_hidrocarburos (
    fact_hidrocarburo_key            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tiempo_key                       BIGINT NOT NULL REFERENCES "Fact_Dim".dim_tiempo(tiempo_key),
    producto_key                     BIGINT NOT NULL REFERENCES "Fact_Dim".dim_producto_hidrocarburo(producto_key),
    resolucion_key                   BIGINT NOT NULL REFERENCES "Fact_Dim".dim_resolucion_hidrocarburo(resolucion_key),

    tipo_cambio                      NUMERIC(18,6),
    precio_referencia_internacional  NUMERIC(18,6),
    precio_colonizado                NUMERIC(18,6),
    otros_ingresos_prorrateados      NUMERIC(18,6),
    asig_cruz_pesc                   NUMERIC(18,6),
    asig_cruz_minae                  NUMERIC(18,6),
    sub_cruz_pesc                    NUMERIC(18,6),
    sub_cruz_minae                   NUMERIC(18,6),
    diferencial_precios              NUMERIC(18,6),
    impuesto_unico                   NUMERIC(18,6),
    canon                            NUMERIC(18,6),
    margen_operacion                 NUMERIC(18,6),
    rend_tarif                       NUMERIC(18,6),
    precio_plantel_sin_impuesto      NUMERIC(18,6),
    precio_con_impuesto              NUMERIC(18,6),
    margen_estaciones_terrestres     NUMERIC(18,6),
    margen_estaciones_aereas         NUMERIC(18,6),
    flete_estaciones                 NUMERIC(18,6),
    margen_envasador                 NUMERIC(18,6),
    margen_distribuidor              NUMERIC(18,6),
    margen_detallista                NUMERIC(18,6),
    precio_consumidor_final_gas      NUMERIC(18,6),
    precio_final                     NUMERIC(18,6),
    precio_final_sin_punto_fijo      NUMERIC(18,6),

    UNIQUE (tiempo_key, producto_key, resolucion_key)
);


-- ============================================
-- DIMENSION ZONA CONCESION
-- ============================================
CREATE TABLE IF NOT EXISTS "Fact_Dim".dim_zona_concesion (
    zona_key        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_objecto      INTEGER NOT NULL UNIQUE,
    operador        VARCHAR(100),
    descripcion     VARCHAR(200),
    area            NUMERIC(18,8),
    coordenadas_wkt TEXT NOT NULL,
    tipo_geometria  VARCHAR(20),
    srid            INTEGER NOT NULL DEFAULT 5367
);

CREATE TABLE IF NOT EXISTS "Fact_Dim".bridge_empresa_zona (
    bridge_empresa_zona_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    empresa_key BIGINT NOT NULL REFERENCES "Fact_Dim".dim_empresa(empresa_key),
    zona_key    BIGINT NOT NULL REFERENCES "Fact_Dim".dim_zona_concesion(zona_key),
    fuente_vinculo VARCHAR(50) NOT NULL DEFAULT 'OPERADOR',
    UNIQUE (empresa_key, zona_key)
);

CREATE TABLE IF NOT EXISTS "Fact_Dim".bridge_central_zona (
    bridge_central_zona_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    central_key BIGINT NOT NULL REFERENCES "Fact_Dim".dim_central_electrica(central_key),
    zona_key    BIGINT NOT NULL REFERENCES "Fact_Dim".dim_zona_concesion(zona_key),
    fuente_vinculo VARCHAR(50) NOT NULL DEFAULT 'OPERADOR_ALIAS',
    UNIQUE (central_key, zona_key)
);

-- ============================================
-- INDICES RECOMENDADOS
-- ============================================
CREATE INDEX IF NOT EXISTS idx_dim_tiempo_anio_mes
ON "Fact_Dim".dim_tiempo (anio, mes);

CREATE INDEX IF NOT EXISTS idx_fact_clima_tiempo_empresa
ON "Fact_Dim".fact_clima_mensual (tiempo_key, empresa_key);

CREATE INDEX IF NOT EXISTS idx_fact_tarifa_tiempo_empresa_tarifa
ON "Fact_Dim".fact_tarifa_electricidad (tiempo_key, empresa_key, tarifa_key);

CREATE INDEX IF NOT EXISTS idx_fact_distribucion_tiempo_empresa_tarifa
ON "Fact_Dim".fact_distribucion_tarifaria (tiempo_key, empresa_key, tarifa_key);

CREATE INDEX IF NOT EXISTS idx_fact_hidrocarburos_tiempo_producto
ON "Fact_Dim".fact_hidrocarburos (tiempo_key, producto_key);

CREATE INDEX IF NOT EXISTS idx_dim_zona_concesion_operador
ON "Fact_Dim".dim_zona_concesion (operador);

CREATE INDEX IF NOT EXISTS idx_bridge_empresa_zona_empresa
ON "Fact_Dim".bridge_empresa_zona (empresa_key);

CREATE INDEX IF NOT EXISTS idx_bridge_empresa_zona_zona
ON "Fact_Dim".bridge_empresa_zona (zona_key);

CREATE INDEX IF NOT EXISTS idx_bridge_central_zona_central
ON "Fact_Dim".bridge_central_zona (central_key);

CREATE INDEX IF NOT EXISTS idx_bridge_central_zona_zona
ON "Fact_Dim".bridge_central_zona (zona_key);
