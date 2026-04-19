-- ============================================
-- VISTA ANALITICA - DATASET FINAL
-- ============================================

DROP VIEW IF EXISTS "Fact_Dim".vw_dataset_final_2020_2025;

CREATE VIEW "Fact_Dim".vw_dataset_final_2020_2025 AS
SELECT
    tp.mes AS "Mes",
    tp.anio AS "Año",
    e.nombre_empresa AS "Empresa",
    dt.nombre_tarifa AS "Tarifa",
    ft.abonados AS "Abonados",
    ft.ventas AS "Ventas",
    ft.ingreso_sin_cvg AS "Ingreso sin CVG",
    ft.ingreso_con_cvg AS "Ingreso con CVG",
    ft.precio_medio_sin_cvg AS "Precio Medio sin CVG",
    ft.precio_medio_con_cvg AS "Precio Medio con CVG",
    CASE tp.trimestre
        WHEN 1 THEN 'I Trimestre'
        WHEN 2 THEN 'II Trimestre'
        WHEN 3 THEN 'III Trimestre'
        WHEN 4 THEN 'IV Trimestre'
        ELSE NULL
    END AS "Trimestre",
    dt.sistema AS "Sistema",
    dt.indicador_trimestral AS "Trimestral",
    vcem.t2m AS "T2M",
    vcem.ws10m AS "WS10M",
    vcem.cloud_amt AS "CLOUD_AMT",
    vcem.rh2m AS "RH2M",
    vcem.t2m_max AS "T2M_MAX",
    vcem.t2m_min AS "T2M_MIN",
    vcem.cloud_od AS "CLOUD_OD",
    vcem.gwetroot AS "GWETROOT",
    vcem.ts AS "TS",
    vcem.prectotcorr AS "PRECTOTCORR",
    vcem.allsky_sfc_sw_dwn AS "ALLSKY_SFC_SW_DWN",
    vcem.ps AS "PS",
    vcem.t2mwet AS "T2MWET",
    vcem.allsky_sfc_sw_diff AS "ALLSKY_SFC_SW_DIFF",
    vcem.allsky_sfc_lw_dwn AS "ALLSKY_SFC_LW_DWN"
FROM "Fact_Dim".fact_tarifa_electricidad ft
INNER JOIN "Fact_Dim".dim_tiempo tp
    ON tp.tiempo_key = ft.tiempo_key
INNER JOIN "Fact_Dim".dim_empresa e
    ON e.empresa_key = ft.empresa_key
INNER JOIN "Fact_Dim".dim_tarifa dt
    ON dt.tarifa_key = ft.tarifa_key
LEFT JOIN "Fact_Dim".vw_clima_empresas_mensual vcem
    ON vcem.nombre_empresa = e.nombre_empresa
   AND vcem.fecha_mes = tp.fecha
ORDER BY
    tp.anio,
    tp.mes,
    e.nombre_empresa,
    dt.nombre_tarifa;
