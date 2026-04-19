-- ============================================
-- VISTAS ANALITICAS - FACT_DIM
-- ============================================

-- Relaciona los ingresos y ventas mensuales de cada empresa con
-- la estructura tarifaria por bloque. El cruce entre ambas fuentes
-- se realiza por empresa, periodo y descripcion de tarifa, ya que
-- los hechos de precios medios y distribucion usan claves naturales
-- distintas dentro de dim_tarifa.
-- Cuando no existe una tarifa por bloque equivalente en distribucion,
-- se usan etiquetas descriptivas y la tarifa_promedio cae al precio
-- medio con CVG, luego al precio medio sin CVG y finalmente a 0.
DROP VIEW IF EXISTS "Fact_Dim".vw_ingresos_empresas_tarifas_mensual;

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
    ec.fuentes_electricas_agregadas,
    ec.centrales_electricas_agregadas,
    ec.operadores_centrales_agregados
FROM tarifa_medios tm
LEFT JOIN tarifa_distribucion td
    ON td.tiempo_key = tm.tiempo_key
   AND td.empresa_key = tm.empresa_key
   AND td.descripcion_join = tm.tarifa_join
LEFT JOIN "Fact_Dim".vw_empresa_centrales_agregadas ec
    ON ec.empresa_canonica = tm.nombre_empresa
ORDER BY
    tm.nombre_empresa,
    tm.fecha_mes,
    COALESCE(td.descripcion_tarifa, tm.tarifa),
    td.tipo_tarifa,
    td.bloque;
