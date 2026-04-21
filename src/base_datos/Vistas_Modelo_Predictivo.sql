-- ============================================
-- VISTA ANALITICA - MODELO PREDICTIVO
-- ============================================

DROP VIEW IF EXISTS "Fact_Dim".vw_modelo_predictivo_central_tarifa_mensual CASCADE;

-- La vista conserva el grano mensual por empresa + sistema + tarifa
-- + descripcion_tarifa + central para exponer una fila unica por
-- serie visible, sin depender de tipo_tarifa ni bloque_consumo.
CREATE VIEW "Fact_Dim".vw_modelo_predictivo_central_tarifa_mensual AS
WITH ingresos_modelo AS (
    SELECT
        it.fecha_anio,
        it.fecha_mes,
        it.anio,
        it.mes,
        it.nombre_mes,
        it.nombre_empresa,
        COALESCE(it.sistema, 'SIN_SISTEMA') AS sistema,
        COALESCE(it.tarifa, 'SIN_TARIFA') AS tarifa,
        COALESCE(it.descripcion_tarifa, it.tarifa, 'SIN_DESCRIPCION_TARIFA') AS descripcion_tarifa,
        MAX(it.abonados) AS abonados,
        MAX(it.ventas_por_mes) AS ventas_por_mes,
        MAX(it.ingreso_sin_cvg) AS ingreso_sin_cvg,
        MAX(it.ingreso_con_cvg) AS ingreso_con_cvg,
        MAX(it.precio_medio_sin_cvg) AS precio_medio_sin_cvg,
        MAX(it.precio_medio_con_cvg) AS precio_medio_con_cvg,
        AVG(it.tarifa_promedio) AS pago_promedio_cliente_tarifa
    FROM "Fact_Dim".vw_ingresos_empresas_tarifas_mensual it
    WHERE it.fecha_mes >= DATE '2018-01-01'
      AND it.fecha_mes < DATE '2026-01-01'
    GROUP BY
        it.fecha_anio,
        it.fecha_mes,
        it.anio,
        it.mes,
        it.nombre_mes,
        it.nombre_empresa,
        COALESCE(it.sistema, 'SIN_SISTEMA'),
        COALESCE(it.tarifa, 'SIN_TARIFA'),
        COALESCE(it.descripcion_tarifa, it.tarifa, 'SIN_DESCRIPCION_TARIFA')
),
empresa_caracteristicas AS (
    SELECT
        nombre_empresa,
        fecha_mes,
        STRING_AGG(DISTINCT UPPER(TRIM(sistema)), ' | ' ORDER BY UPPER(TRIM(sistema))) AS sistema,
        STRING_AGG(DISTINCT UPPER(TRIM(tarifa)), ' | ' ORDER BY UPPER(TRIM(tarifa))) AS tarifa,
        STRING_AGG(
            DISTINCT UPPER(TRIM(descripcion_tarifa)),
            ' | '
            ORDER BY UPPER(TRIM(descripcion_tarifa))
        ) AS descripcion_tarifa
    FROM ingresos_modelo
    GROUP BY nombre_empresa, fecha_mes
),
ingresos_modelo_mes AS (
    SELECT
        fecha_anio,
        fecha_mes,
        anio,
        mes,
        nombre_mes,
        nombre_empresa,
        SUM(abonados) AS abonados,
        SUM(ventas_por_mes) AS ventas_por_mes,
        SUM(ingreso_sin_cvg) AS ingreso_sin_cvg,
        SUM(ingreso_con_cvg) AS ingreso_con_cvg,
        CASE
            WHEN SUM(ventas_por_mes) = 0 THEN 0
            ELSE SUM(ingreso_sin_cvg) / SUM(ventas_por_mes)
        END AS precio_medio_sin_cvg,
        CASE
            WHEN SUM(ventas_por_mes) = 0 THEN 0
            ELSE SUM(ingreso_con_cvg) / SUM(ventas_por_mes)
        END AS precio_medio_con_cvg,
        AVG(pago_promedio_cliente_tarifa) AS pago_promedio_cliente_tarifa
    FROM ingresos_modelo
    GROUP BY
        fecha_anio,
        fecha_mes,
        anio,
        mes,
        nombre_mes,
        nombre_empresa
),
base_modelo AS (
    SELECT
        cc.fecha_anio,
        cc.fecha_mes,
        TO_CHAR(cc.fecha_mes, 'YYYY-MM') AS periodo_yyyy_mm,
        cc.anio,
        cc.mes,
        EXTRACT(QUARTER FROM cc.fecha_mes)::INTEGER AS trimestre,
        cc.nombre_mes,
        cc.nombre_empresa,
        ec.sistema,
        ec.tarifa,
        ec.descripcion_tarifa,
        cc.id_objecto,
        cc.central_electrica,
        cc.operador,
        cc.fuente,
        cc.provincia,
        cc.canton,
        cc.distrito,
        cc.codigo_dta,
        cc.coordenada_x,
        cc.coordenada_y,
        COALESCE(im.abonados, 0) AS abonados,
        COALESCE(im.ventas_por_mes, 0) AS ventas_por_mes,
        COALESCE(im.ingreso_sin_cvg, 0) AS ingreso_sin_cvg,
        COALESCE(im.ingreso_con_cvg, 0) AS ingreso_con_cvg,
        COALESCE(im.precio_medio_sin_cvg, 0) AS precio_medio_sin_cvg,
        COALESCE(im.precio_medio_con_cvg, 0) AS precio_medio_con_cvg,
        COALESCE(im.pago_promedio_cliente_tarifa, 0) AS pago_promedio_cliente_tarifa,
        cc.t2m,
        cc.ws10m,
        cc.cloud_amt,
        cc.rh2m,
        cc.t2m_max,
        cc.t2m_min,
        cc.cloud_od,
        cc.gwetroot,
        cc.ts,
        cc.prectotcorr,
        cc.allsky_sfc_sw_dwn,
        cc.ps,
        cc.t2mwet,
        cc.allsky_sfc_sw_diff,
        cc.allsky_sfc_lw_dwn
    FROM "Fact_Dim".vw_clima_centrales_mensual cc
    INNER JOIN ingresos_modelo_mes im
        ON im.nombre_empresa = cc.nombre_empresa
       AND im.fecha_mes = cc.fecha_mes
    INNER JOIN empresa_caracteristicas ec
        ON ec.nombre_empresa = cc.nombre_empresa
       AND ec.fecha_mes = cc.fecha_mes
    WHERE cc.fecha_mes >= DATE '2018-01-01'
      AND cc.fecha_mes < DATE '2026-01-01'
),
base_modelo_imputado AS (
    SELECT
        fecha_anio,
        fecha_mes,
        periodo_yyyy_mm,
        anio,
        mes,
        trimestre,
        nombre_mes,
        nombre_empresa,
        sistema,
        tarifa,
        descripcion_tarifa,
        id_objecto,
        central_electrica,
        operador,
        fuente,
        provincia,
        canton,
        distrito,
        codigo_dta,
        coordenada_x,
        coordenada_y,
        abonados,
        ventas_por_mes,
        ingreso_sin_cvg,
        ingreso_con_cvg,
        precio_medio_sin_cvg,
        precio_medio_con_cvg,
        pago_promedio_cliente_tarifa,
        t2m,
        ws10m,
        CASE
            WHEN cloud_amt = -999
                THEN AVG(NULLIF(cloud_amt, -999)) OVER ()
            ELSE cloud_amt
        END AS cloud_amt,
        rh2m,
        t2m_max,
        t2m_min,
        CASE
            WHEN cloud_od = -999
                THEN AVG(NULLIF(cloud_od, -999)) OVER ()
            ELSE cloud_od
        END AS cloud_od,
        gwetroot,
        ts,
        prectotcorr,
        CASE
            WHEN allsky_sfc_sw_dwn = -999
                THEN AVG(NULLIF(allsky_sfc_sw_dwn, -999)) OVER ()
            ELSE allsky_sfc_sw_dwn
        END AS allsky_sfc_sw_dwn,
        ps,
        t2mwet,
        CASE
            WHEN allsky_sfc_sw_diff = -999
                THEN AVG(NULLIF(allsky_sfc_sw_diff, -999)) OVER ()
            ELSE allsky_sfc_sw_diff
        END AS allsky_sfc_sw_diff,
        CASE
            WHEN allsky_sfc_lw_dwn = -999
                THEN AVG(NULLIF(allsky_sfc_lw_dwn, -999)) OVER ()
            ELSE allsky_sfc_lw_dwn
        END AS allsky_sfc_lw_dwn
    FROM base_modelo
)
SELECT
    anio,
    mes,
    trimestre,
    nombre_mes,
    nombre_empresa,
    sistema,
    tarifa,
    descripcion_tarifa,
    id_objecto,
    central_electrica,
    operador,
    fuente,
    provincia,
    canton,
    distrito,
    codigo_dta,
    coordenada_x,
    coordenada_y,
    abonados,
    ventas_por_mes,
    ingreso_sin_cvg,
    ingreso_con_cvg,
    precio_medio_sin_cvg,
    precio_medio_con_cvg,
    pago_promedio_cliente_tarifa,
    t2m,
    ws10m,
    cloud_amt,
    rh2m,
    t2m_max,
    t2m_min,
    cloud_od,
    gwetroot,
    ts,
    prectotcorr,
    allsky_sfc_sw_dwn,
    ps,
    t2mwet,
    allsky_sfc_sw_diff,
    allsky_sfc_lw_dwn
FROM base_modelo_imputado
ORDER BY
    fecha_mes DESC,
    nombre_empresa,
    sistema,
    tarifa,
    descripcion_tarifa,
    id_objecto;
