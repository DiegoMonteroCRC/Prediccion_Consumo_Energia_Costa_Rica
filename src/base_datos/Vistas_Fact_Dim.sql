-- ============================================
-- VISTAS ANALITICAS - FACT_DIM
-- ============================================

DROP VIEW IF EXISTS "Fact_Dim".vw_dataset_final_2020_2025;
DROP VIEW IF EXISTS "Fact_Dim".vw_ingresos_empresas_tarifas_mensual CASCADE;
DROP VIEW IF EXISTS "Fact_Dim".vw_clima_empresas_mensual CASCADE;
DROP VIEW IF EXISTS "Fact_Dim".vw_clima_centrales_mensual CASCADE;

-- Expone el hecho de clima en su grano actual central + mes
-- junto con metadatos de empresa, central y ubicacion.
CREATE VIEW "Fact_Dim".vw_clima_centrales_mensual AS
SELECT
    e.nombre_empresa,
    tp.fecha AS fecha_mes,
    tp.anio,
    tp.mes,
    tp.nombre_mes,
    ce.id_objecto,
    ce.central_electrica,
    ce.operador,
    ce.fuente,
    u.provincia,
    u.canton,
    u.distrito,
    u.codigo_dta,
    u.coordenada_x,
    u.coordenada_y,
    fc.t2m,
    fc.ws10m,
    fc.cloud_amt,
    fc.rh2m,
    fc.t2m_max,
    fc.t2m_min,
    fc.cloud_od,
    fc.gwetroot,
    fc.ts,
    fc.prectotcorr,
    fc.allsky_sfc_sw_dwn,
    fc.ps,
    fc.t2mwet,
    fc.allsky_sfc_sw_diff,
    fc.allsky_sfc_lw_dwn
FROM "Fact_Dim".fact_clima_mensual fc
INNER JOIN "Fact_Dim".dim_tiempo tp
    ON tp.tiempo_key = fc.tiempo_key
INNER JOIN "Fact_Dim".dim_empresa e
    ON e.empresa_key = fc.empresa_key
INNER JOIN "Fact_Dim".dim_central_electrica ce
    ON ce.central_key = fc.central_key
LEFT JOIN "Fact_Dim".dim_ubicacion u
    ON u.ubicacion_key = ce.ubicacion_key;

-- Agrega el clima mensual por empresa para mantener una capa de consumo
-- equivalente al dataset analitico historico sin perder la trazabilidad
-- hacia las centrales que alimentaron cada promedio mensual.
CREATE VIEW "Fact_Dim".vw_clima_empresas_mensual AS
SELECT
    e.nombre_empresa,
    tp.fecha AS fecha_mes,
    tp.anio,
    tp.mes,
    tp.nombre_mes,
    COUNT(DISTINCT fc.central_key) AS centrales_con_clima,
    AVG(fc.t2m) AS t2m,
    AVG(fc.ws10m) AS ws10m,
    AVG(fc.cloud_amt) AS cloud_amt,
    AVG(fc.rh2m) AS rh2m,
    AVG(fc.t2m_max) AS t2m_max,
    AVG(fc.t2m_min) AS t2m_min,
    AVG(fc.cloud_od) AS cloud_od,
    AVG(fc.gwetroot) AS gwetroot,
    AVG(fc.ts) AS ts,
    AVG(fc.prectotcorr) AS prectotcorr,
    AVG(fc.allsky_sfc_sw_dwn) AS allsky_sfc_sw_dwn,
    AVG(fc.ps) AS ps,
    AVG(fc.t2mwet) AS t2mwet,
    AVG(fc.allsky_sfc_sw_diff) AS allsky_sfc_sw_diff,
    AVG(fc.allsky_sfc_lw_dwn) AS allsky_sfc_lw_dwn
FROM "Fact_Dim".fact_clima_mensual fc
INNER JOIN "Fact_Dim".dim_tiempo tp
    ON tp.tiempo_key = fc.tiempo_key
INNER JOIN "Fact_Dim".dim_empresa e
    ON e.empresa_key = fc.empresa_key
GROUP BY
    e.nombre_empresa,
    tp.fecha,
    tp.anio,
    tp.mes,
    tp.nombre_mes;

-- Relaciona los ingresos y ventas mensuales de cada empresa con
-- la estructura tarifaria por bloque y el clima promedio mensual.
-- El cruce entre tarifas se realiza por empresa, periodo y descripcion,
-- mientras que el clima se integra a nivel empresa + mes.
CREATE VIEW "Fact_Dim".vw_ingresos_empresas_tarifas_mensual AS
WITH tarifa_medios_base AS (
    SELECT
        ft.tiempo_key,
        ft.empresa_key,
        e.nombre_empresa,
        tp.fecha AS fecha_mes,
        tp.anio,
        tp.mes,
        tp.nombre_mes,
        dt.nombre_tarifa AS tarifa,
        CASE
            WHEN UPPER(BTRIM(dt.nombre_tarifa)) = 'RESIDENCIAL MODALIDAD PREPAGO'
                THEN 'TARIFA RESIDENCIAL MODALIDAD PREPAGO'
            ELSE UPPER(BTRIM(dt.nombre_tarifa))
        END AS tarifa_join,
        dt.sistema,
        ft.abonados,
        ft.ventas,
        ft.ingreso_sin_cvg,
        ft.ingreso_con_cvg,
        ft.precio_medio_sin_cvg,
        ft.precio_medio_con_cvg
    FROM "Fact_Dim".fact_tarifa_electricidad ft
    INNER JOIN "Fact_Dim".dim_tiempo tp
        ON tp.tiempo_key = ft.tiempo_key
    INNER JOIN "Fact_Dim".dim_empresa e
        ON e.empresa_key = ft.empresa_key
    INNER JOIN "Fact_Dim".dim_tarifa dt
        ON dt.tarifa_key = ft.tarifa_key
),
tarifa_medios AS (
    SELECT
        tiempo_key,
        empresa_key,
        nombre_empresa,
        fecha_mes,
        anio,
        mes,
        nombre_mes,
        tarifa,
        tarifa_join,
        sistema,
        SUM(abonados) AS abonados,
        SUM(ventas) AS ventas,
        SUM(ingreso_sin_cvg) AS ingreso_sin_cvg,
        SUM(ingreso_con_cvg) AS ingreso_con_cvg,
        AVG(precio_medio_sin_cvg) AS precio_medio_sin_cvg,
        AVG(precio_medio_con_cvg) AS precio_medio_con_cvg
    FROM tarifa_medios_base
    GROUP BY
        tiempo_key,
        empresa_key,
        nombre_empresa,
        fecha_mes,
        anio,
        mes,
        nombre_mes,
        tarifa,
        tarifa_join,
        sistema
),
tarifa_distribucion_base AS (
    SELECT
        fd.tiempo_key,
        fd.empresa_key,
        e.nombre_empresa,
        tp.fecha AS fecha_mes,
        tp.anio,
        tp.mes,
        tp.nombre_mes,
        UPPER(BTRIM(dt.descripcion_tarifa)) AS descripcion_join,
        dt.descripcion_tarifa,
        dt.tipo_tarifa,
        dt.bloque,
        fd.tarifa_promedio
    FROM "Fact_Dim".fact_distribucion_tarifaria fd
    INNER JOIN "Fact_Dim".dim_tiempo tp
        ON tp.tiempo_key = fd.tiempo_key
    INNER JOIN "Fact_Dim".dim_empresa e
        ON e.empresa_key = fd.empresa_key
    INNER JOIN "Fact_Dim".dim_tarifa dt
        ON dt.tarifa_key = fd.tarifa_key
),
tarifa_distribucion AS (
    SELECT
        tiempo_key,
        empresa_key,
        nombre_empresa,
        fecha_mes,
        anio,
        mes,
        nombre_mes,
        descripcion_join,
        descripcion_tarifa,
        tipo_tarifa,
        bloque,
        AVG(tarifa_promedio) AS tarifa_promedio
    FROM tarifa_distribucion_base
    GROUP BY
        tiempo_key,
        empresa_key,
        nombre_empresa,
        fecha_mes,
        anio,
        mes,
        nombre_mes,
        descripcion_join,
        descripcion_tarifa,
        tipo_tarifa,
        bloque
)
SELECT
    tm.nombre_empresa,
    tm.fecha_mes,
    MAKE_DATE(tm.anio, 1, 1) AS fecha_anio,
    tm.anio,
    tm.mes,
    tm.nombre_mes,
    tm.tarifa,
    COALESCE(td.descripcion_tarifa, tm.tarifa) AS descripcion_tarifa,
    COALESCE(td.tipo_tarifa, 'SIN CORRESPONDENCIA'::VARCHAR(50)) AS tipo_tarifa,
    COALESCE(td.bloque, 'SIN BLOQUE ASOCIADO'::VARCHAR(100)) AS bloque_consumo,
    tm.sistema,
    COALESCE(tm.abonados, 0) AS abonados,
    COALESCE(td.tarifa_promedio, tm.precio_medio_con_cvg, tm.precio_medio_sin_cvg, 0) AS tarifa_promedio,
    tm.ventas AS ventas_por_mes,
    tm.ingreso_sin_cvg,
    tm.ingreso_con_cvg,
    tm.precio_medio_sin_cvg,
    tm.precio_medio_con_cvg,
    tm.precio_medio_con_cvg AS "Pago_kw/h_Promedio_clienteXtarifa",
    ec.centrales_con_clima,
    ec.t2m,
    ec.ws10m,
    ec.cloud_amt,
    ec.rh2m,
    ec.t2m_max,
    ec.t2m_min,
    ec.cloud_od,
    ec.gwetroot,
    ec.ts,
    ec.prectotcorr,
    ec.allsky_sfc_sw_dwn,
    ec.ps,
    ec.t2mwet,
    ec.allsky_sfc_sw_diff,
    ec.allsky_sfc_lw_dwn,
    ee.cantidad_centrales_asociadas,
    ee.fuentes_electricas_agregadas,
    ee.centrales_electricas_agregadas,
    ee.operadores_centrales_agregados,
    ee.coordenadas_x_agregadas,
    ee.coordenadas_y_agregadas,
    ee.coordenadas_xy_agregadas
FROM tarifa_medios tm
LEFT JOIN tarifa_distribucion td
    ON td.tiempo_key = tm.tiempo_key
   AND td.empresa_key = tm.empresa_key
   AND td.descripcion_join = tm.tarifa_join
LEFT JOIN "Fact_Dim".vw_clima_empresas_mensual ec
    ON ec.nombre_empresa = tm.nombre_empresa
   AND ec.fecha_mes = tm.fecha_mes
LEFT JOIN "Fact_Dim".vw_empresa_centrales_agregadas ee
    ON ee.empresa_canonica = tm.nombre_empresa
ORDER BY
    tm.nombre_empresa,
    tm.fecha_mes,
    COALESCE(td.descripcion_tarifa, tm.tarifa),
    td.tipo_tarifa,
    td.bloque;
