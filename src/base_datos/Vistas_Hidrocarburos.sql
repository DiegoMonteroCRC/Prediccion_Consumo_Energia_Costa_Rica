-- ============================================
-- VISTA ANALITICA - HIDROCARBUROS
-- ============================================

DROP VIEW IF EXISTS "Fact_Dim".vw_hidrocarburos_detalle CASCADE;

-- Expone el hecho de hidrocarburos junto con sus dimensiones de
-- tiempo, producto y resolucion para consultas analiticas directas.
CREATE VIEW "Fact_Dim".vw_hidrocarburos_detalle AS
SELECT
    TO_CHAR(tp.fecha, 'YYYY') AS anio,
    tp.mes,
    tp.dia,
    tp.trimestre,
    tp.nombre_mes,
    ph.producto,
    rh.numero_expediente,
    rh.numero_resolucion,
    rh.alcance_gaceta,
    rh.numero_gaceta,
    rh.rige,
    fh.tipo_cambio,
    fh.precio_referencia_internacional,
    fh.precio_colonizado,
    fh.otros_ingresos_prorrateados,
    fh.asig_cruz_pesc,
    fh.asig_cruz_minae,
    fh.sub_cruz_pesc,
    fh.sub_cruz_minae,
    fh.diferencial_precios,
    fh.impuesto_unico,
    fh.canon,
    fh.margen_operacion,
    fh.rend_tarif,
    fh.precio_plantel_sin_impuesto,
    fh.precio_con_impuesto,
    fh.margen_estaciones_terrestres,
    fh.margen_estaciones_aereas,
    fh.flete_estaciones,
    fh.margen_envasador,
    fh.margen_distribuidor,
    fh.margen_detallista,
    fh.precio_consumidor_final_gas,
    fh.precio_final,
    fh.precio_final_sin_punto_fijo
FROM "Fact_Dim".fact_hidrocarburos fh
INNER JOIN "Fact_Dim".dim_tiempo tp
    ON tp.tiempo_key = fh.tiempo_key
INNER JOIN "Fact_Dim".dim_producto_hidrocarburo ph
    ON ph.producto_key = fh.producto_key
INNER JOIN "Fact_Dim".dim_resolucion_hidrocarburo rh
    ON rh.resolucion_key = fh.resolucion_key
ORDER BY
    tp.fecha DESC,
    ph.producto,
    rh.numero_resolucion;
