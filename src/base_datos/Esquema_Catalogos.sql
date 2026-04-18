-- ============================================
-- ESQUEMA: Catalogo
-- Propósito:
--   Almacenar catálogos/diccionarios de variables
--   provenientes de fuentes ARESEP y NASA POWER
-- ============================================

CREATE SCHEMA IF NOT EXISTS "Catalogo";

-- ============================================
-- 1) aresep_centrales_electricas_variables.csv
-- ============================================

CREATE TABLE IF NOT EXISTS "Catalogo".catalogo_centrales_electricas_variables (
    id                        VARCHAR(100),
    name                      VARCHAR(255),
    tags                      TEXT,
    services                  VARCHAR(100),
    availability              VARCHAR(100),
    description               TEXT,
    keywords                  TEXT,
    warnings_api_original     TEXT
);

-- ============================================
-- 2) aresep_hidrocarburos_variables.csv
-- ============================================

CREATE TABLE IF NOT EXISTS "Catalogo".catalogo_hidrocarburos_variables (
    id                        VARCHAR(100),
    name                      VARCHAR(255),
    tags                      TEXT,
    services                  VARCHAR(100),
    availability              VARCHAR(100),
    description               TEXT,
    keywords                  TEXT,
    warnings_api_original     TEXT
);

-- ============================================
-- 3) aresep_tarifas_electricidad_distribucion_variables.csv
-- ============================================

CREATE TABLE IF NOT EXISTS "Catalogo".catalogo_tarifas_electricidad_distribucion_variables (
    id                        VARCHAR(100),
    name                      VARCHAR(255),
    tags                      TEXT,
    services                  VARCHAR(100),
    availability              VARCHAR(100),
    description               TEXT,
    keywords                  TEXT,
    warnings_api_original     TEXT
);

-- ============================================
-- 4) aresep_tarifas_precios_medios_variables.csv
-- ============================================

CREATE TABLE IF NOT EXISTS "Catalogo".catalogo_tarifas_precios_medios_variables (
    id                        VARCHAR(100),
    name                      VARCHAR(255),
    tags                      TEXT,
    services                  VARCHAR(100),
    availability              VARCHAR(100),
    description               TEXT,
    keywords                  TEXT,
    warnings_api_original     TEXT
);

-- ============================================
-- 5) nasa_power_parameters_name_es.csv
-- ============================================

CREATE TABLE IF NOT EXISTS "Catalogo".catalogo_clima_variables (
    id                        VARCHAR(100),
    name_es                   VARCHAR(255),
    name_en                   VARCHAR(255),
    tags                      TEXT,
    services                  VARCHAR(100),
    availability              VARCHAR(150),
    description               TEXT,
    alternates                TEXT,
    keywords                  TEXT,
    warnings                  TEXT
);

CREATE TABLE IF NOT EXISTS "Catalogo".catalogo_zonas_concesion_operador_variables (
    id                        VARCHAR(100),
    name                      VARCHAR(255),
    tags                      TEXT,
    services                  VARCHAR(100),
    availability              VARCHAR(100),
    description               TEXT,
    keywords                  TEXT,
    warnings_api_original     TEXT
);

