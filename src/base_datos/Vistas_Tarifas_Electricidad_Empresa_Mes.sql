-- ============================================
-- VISTA ANALITICA - TARIFAS ELECTRICIDAD
-- ============================================

DROP VIEW IF EXISTS "Fact_Dim".vw_tarifas_electricidad_empresa_mes CASCADE;
DROP VIEW IF EXISTS "Fact_Dim".prediccion_consumo_empresa CASCADE;

-- Resume las tarifas electricas al grano empresa + mes + tarifa,
-- con abonados del hecho tarifario y cantidad de centrales observadas
-- para la misma empresa y periodo mensual.
CREATE VIEW "Fact_Dim".prediccion_consumo_empresa AS
WITH centrales_empresa_mes AS (
    SELECT
        fc.tiempo_key,
        fc.empresa_key,
        COUNT(DISTINCT fc.central_key) AS empresa_mes_cantidad_centrales
    FROM "Fact_Dim".fact_clima_mensual fc
    WHERE fc.central_key IS NOT NULL
    GROUP BY
        fc.tiempo_key,
        fc.empresa_key
),
tarifas_empresa_mes AS (
    SELECT
        ft.tiempo_key,
        ft.empresa_key,
        ft.tarifa_key,
        e.nombre_empresa,
        tp.fecha AS fecha_mes,
        DATE_TRUNC('year', tp.fecha)::DATE AS fecha_anio,
        tp.anio::VARCHAR(4) AS anio,
        tp.mes,
        EXTRACT(QUARTER FROM tp.fecha)::INTEGER AS trimestre,
        tp.nombre_mes,
        COALESCE(dt.nombre_tarifa, dt.descripcion_tarifa, 'SIN_TARIFA') AS tarifa_electricidad,
        dt.sistema,
        SUM(ft.abonados) AS empresa_mes_abonados_total,
        SUM(ft.ventas) AS empresa_mes_tarifa_consumo_energia_kwh_total,
        SUM(ft.ventas) / NULLIF(SUM(ft.abonados), 0) AS empresa_mes_tarifa_consumo_energia_kwh_promedio_por_abonado,
        SUM(ft.ingreso_sin_cvg) AS empresa_mes_tarifa_ingreso_sin_cvg_crc_total,
        SUM(ft.ingreso_con_cvg) AS empresa_mes_tarifa_ingreso_con_cvg_crc_total,
        SUM(ft.ingreso_sin_cvg) / NULLIF(SUM(ft.abonados), 0) AS empresa_mes_tarifa_pago_sin_cvg_crc_promedio_por_abonado,
        SUM(ft.ingreso_con_cvg) / NULLIF(SUM(ft.abonados), 0) AS empresa_mes_tarifa_pago_con_cvg_crc_promedio_por_abonado,
        AVG(ft.precio_medio_sin_cvg) AS empresa_mes_tarifa_precio_medio_sin_cvg_crc_kwh_promedio,
        AVG(ft.precio_medio_con_cvg) AS empresa_mes_tarifa_precio_medio_con_cvg_crc_kwh_promedio
    FROM "Fact_Dim".fact_tarifa_electricidad ft
    INNER JOIN "Fact_Dim".dim_tiempo tp
        ON tp.tiempo_key = ft.tiempo_key
    INNER JOIN "Fact_Dim".dim_empresa e
        ON e.empresa_key = ft.empresa_key
    INNER JOIN "Fact_Dim".dim_tarifa dt
        ON dt.tarifa_key = ft.tarifa_key
    GROUP BY
        ft.tiempo_key,
        ft.empresa_key,
        ft.tarifa_key,
        e.nombre_empresa,
        tp.fecha,
        tp.anio,
        tp.mes,
        tp.nombre_mes,
        COALESCE(dt.nombre_tarifa, dt.descripcion_tarifa, 'SIN_TARIFA'),
        dt.sistema
)
SELECT
    tem.fecha_mes,
    tem.fecha_anio,
    tem.anio,
    tem.mes,
    tem.trimestre,
    tem.nombre_mes,
    tem.nombre_empresa,
    tem.tarifa_electricidad,
    tem.sistema,
    tem.empresa_mes_abonados_total,
    tem.empresa_mes_tarifa_consumo_energia_kwh_total,
    tem.empresa_mes_tarifa_consumo_energia_kwh_promedio_por_abonado,
    tem.empresa_mes_tarifa_ingreso_sin_cvg_crc_total,
    tem.empresa_mes_tarifa_ingreso_con_cvg_crc_total,
    tem.empresa_mes_tarifa_pago_sin_cvg_crc_promedio_por_abonado,
    tem.empresa_mes_tarifa_pago_con_cvg_crc_promedio_por_abonado,
    tem.empresa_mes_tarifa_precio_medio_sin_cvg_crc_kwh_promedio,
    tem.empresa_mes_tarifa_precio_medio_con_cvg_crc_kwh_promedio,
    COALESCE(cem.empresa_mes_cantidad_centrales, 0) AS empresa_mes_cantidad_centrales
FROM tarifas_empresa_mes tem
LEFT JOIN centrales_empresa_mes cem
    ON cem.tiempo_key = tem.tiempo_key
   AND cem.empresa_key = tem.empresa_key
ORDER BY
    tem.fecha_mes,
    tem.nombre_empresa,
    tem.tarifa_electricidad;
