-- ============================================
-- VISTAS ANALITICAS - EMPRESA / CENTRALES
-- ============================================

-- Consolida la relacion entre empresa y centrales electricas
-- sin multiplicar filas en vistas de consumo mensual.
-- La normalizacion de alias sigue el criterio ya usado
-- en Funciones_Fact_Dim.sql para vincular operadores.
CREATE OR REPLACE VIEW "Fact_Dim".vw_empresa_centrales_agregadas AS
WITH alias_empresa AS (
    SELECT *
    FROM (
        VALUES
            ('CNFL'::VARCHAR(100), 'CNFL'::VARCHAR(100)),
            ('ICE'::VARCHAR(100), 'ICE'::VARCHAR(100)),
            ('JASEC'::VARCHAR(100), 'JASEC'::VARCHAR(100)),
            ('ESPH'::VARCHAR(100), 'ESPH'::VARCHAR(100)),
            ('COOPELESCA'::VARCHAR(100), 'COOPELESCA'::VARCHAR(100)),
            ('COOPESANTOS'::VARCHAR(100), 'COOPESANTOS'::VARCHAR(100)),
            ('COOPEALFARORUIZ'::VARCHAR(100), 'COOPEALFARORUIZ'::VARCHAR(100)),
            ('COOPEGUANACASTE'::VARCHAR(100), 'COOPEGUANACASTE R.L.'::VARCHAR(100))
    ) AS alias_empresa(empresa_canonica, empresa_relacionada)
),
empresa_normalizada AS (
    SELECT
        e.nombre_empresa AS empresa_canonica,
        COALESCE(a.empresa_relacionada, e.nombre_empresa) AS empresa_relacionada
    FROM "Fact_Dim".dim_empresa e
    LEFT JOIN alias_empresa a
        ON a.empresa_canonica = e.nombre_empresa
),
empresa_base AS (
    SELECT
        en.empresa_canonica,
        er.empresa_key AS empresa_relacionada_key
    FROM empresa_normalizada en
    LEFT JOIN "Fact_Dim".dim_empresa er
        ON er.nombre_empresa = en.empresa_relacionada
)
SELECT
    eb.empresa_canonica,
    string_agg(DISTINCT ce.fuente, ' | ' ORDER BY ce.fuente)
        FILTER (WHERE ce.fuente IS NOT NULL) AS fuentes_electricas_agregadas,
    string_agg(DISTINCT ce.central_electrica, ' | ' ORDER BY ce.central_electrica)
        FILTER (WHERE ce.central_electrica IS NOT NULL) AS centrales_electricas_agregadas,
    string_agg(DISTINCT ce.operador, ' | ' ORDER BY ce.operador)
        FILTER (WHERE ce.operador IS NOT NULL) AS operadores_centrales_agregados,
    string_agg(DISTINCT u.coordenada_x::TEXT, ' | ' ORDER BY u.coordenada_x::TEXT)
        FILTER (WHERE u.coordenada_x IS NOT NULL) AS coordenadas_x_agregadas,
    string_agg(DISTINCT u.coordenada_y::TEXT, ' | ' ORDER BY u.coordenada_y::TEXT)
        FILTER (WHERE u.coordenada_y IS NOT NULL) AS coordenadas_y_agregadas,
    string_agg(
        DISTINCT (u.coordenada_x::TEXT || ', ' || u.coordenada_y::TEXT),
        ' | '
        ORDER BY (u.coordenada_x::TEXT || ', ' || u.coordenada_y::TEXT)
    ) FILTER (
        WHERE u.coordenada_x IS NOT NULL
          AND u.coordenada_y IS NOT NULL
    ) AS coordenadas_xy_agregadas
FROM empresa_base eb
LEFT JOIN "Fact_Dim".bridge_empresa_zona bez
    ON bez.empresa_key = eb.empresa_relacionada_key
LEFT JOIN "Fact_Dim".bridge_central_zona bcz
    ON bcz.zona_key = bez.zona_key
LEFT JOIN "Fact_Dim".dim_central_electrica ce
    ON ce.central_key = bcz.central_key
LEFT JOIN "Fact_Dim".dim_ubicacion u
    ON u.ubicacion_key = ce.ubicacion_key
GROUP BY
    eb.empresa_canonica
ORDER BY
    eb.empresa_canonica;
