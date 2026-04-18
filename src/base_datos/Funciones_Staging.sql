CREATE OR REPLACE FUNCTION "Staging".fn_stg_insert_clima_nasa(
    p_empresa VARCHAR,
    p_ano INTEGER,
    p_mes INTEGER,
    p_t2m DOUBLE PRECISION,
    p_ws10m DOUBLE PRECISION,
    p_cloud_amt DOUBLE PRECISION,
    p_rh2m DOUBLE PRECISION,
    p_t2m_max DOUBLE PRECISION,
    p_t2m_min DOUBLE PRECISION,
    p_cloud_od DOUBLE PRECISION,
    p_gwetroot DOUBLE PRECISION,
    p_ts DOUBLE PRECISION,
    p_prectotcorr DOUBLE PRECISION,
    p_allsky_sfc_sw_dwn DOUBLE PRECISION,
    p_ps DOUBLE PRECISION,
    p_t2mwet DOUBLE PRECISION,
    p_allsky_sfc_sw_diff DOUBLE PRECISION,
    p_allsky_sfc_lw_dwn DOUBLE PRECISION
)
RETURNS TABLE(ok BOOLEAN, mensaje TEXT, tabla TEXT, filas_afectadas INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO "Staging".stg_clima_nasa (
        empresa, ano, mes, t2m, ws10m, cloud_amt, rh2m, t2m_max, t2m_min,
        cloud_od, gwetroot, ts, prectotcorr, allsky_sfc_sw_dwn, ps, t2mwet,
        allsky_sfc_sw_diff, allsky_sfc_lw_dwn
    )
    VALUES (
        p_empresa, p_ano, p_mes, p_t2m, p_ws10m, p_cloud_amt, p_rh2m, p_t2m_max, p_t2m_min,
        p_cloud_od, p_gwetroot, p_ts, p_prectotcorr, p_allsky_sfc_sw_dwn, p_ps, p_t2mwet,
        p_allsky_sfc_sw_diff, p_allsky_sfc_lw_dwn
    );

    RETURN QUERY SELECT TRUE, 'Fila insertada correctamente', 'stg_clima_nasa', 1;
EXCEPTION
    WHEN OTHERS THEN
        RETURN QUERY SELECT FALSE, SQLERRM, 'stg_clima_nasa', 0;
END;
$$;

CREATE OR REPLACE FUNCTION "Staging".fn_stg_insert_aresep_medios(
    p_mes INTEGER,
    p_ano INTEGER,
    p_empresa VARCHAR,
    p_tarifa VARCHAR,
    p_abonados DOUBLE PRECISION,
    p_ventas DOUBLE PRECISION,
    p_ingreso_sin_cvg DOUBLE PRECISION,
    p_ingreso_con_cvg DOUBLE PRECISION,
    p_precio_medio_sin_cvg DOUBLE PRECISION,
    p_precio_medio_con_cvg DOUBLE PRECISION,
    p_trimestre VARCHAR,
    p_sistema VARCHAR,
    p_trimestral VARCHAR
)
RETURNS TABLE(ok BOOLEAN, mensaje TEXT, tabla TEXT, filas_afectadas INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO "Staging".stg_aresep_medios (
        mes, ano, empresa, tarifa, abonados, ventas, ingreso_sin_cvg, ingreso_con_cvg,
        precio_medio_sin_cvg, precio_medio_con_cvg, trimestre, sistema, trimestral
    )
    VALUES (
        p_mes, p_ano, p_empresa, p_tarifa, p_abonados, p_ventas, p_ingreso_sin_cvg, p_ingreso_con_cvg,
        p_precio_medio_sin_cvg, p_precio_medio_con_cvg, p_trimestre, p_sistema, p_trimestral
    );

    RETURN QUERY SELECT TRUE, 'Fila insertada correctamente', 'stg_aresep_medios', 1;
EXCEPTION
    WHEN OTHERS THEN
        RETURN QUERY SELECT FALSE, SQLERRM, 'stg_aresep_medios', 0;
END;
$$;

CREATE OR REPLACE FUNCTION "Staging".fn_stg_insert_centro(
    p_id_objecto INTEGER,
    p_operador VARCHAR,
    p_central_electrica VARCHAR,
    p_fuente VARCHAR,
    p_provincia VARCHAR,
    p_canton VARCHAR,
    p_distrito VARCHAR,
    p_codigo_dta INTEGER,
    p_coordenada_x DOUBLE PRECISION,
    p_coordenada_y DOUBLE PRECISION
)
RETURNS TABLE(ok BOOLEAN, mensaje TEXT, tabla TEXT, filas_afectadas INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO "Staging".stg_centro (
        id_objecto, operador, central_electrica, fuente, provincia, canton,
        distrito, codigo_dta, coordenada_x, coordenada_y
    )
    VALUES (
        p_id_objecto, p_operador, p_central_electrica, p_fuente, p_provincia, p_canton,
        p_distrito, p_codigo_dta, p_coordenada_x, p_coordenada_y
    );

    RETURN QUERY SELECT TRUE, 'Fila insertada correctamente', 'stg_centro', 1;
EXCEPTION
    WHEN OTHERS THEN
        RETURN QUERY SELECT FALSE, SQLERRM, 'stg_centro', 0;
END;
$$;

CREATE OR REPLACE FUNCTION "Staging".fn_stg_insert_distribucion(
    p_id_mes INTEGER,
    p_mes VARCHAR,
    p_anho INTEGER,
    p_fecha DATE,
    p_empresa VARCHAR,
    p_tipo_tarifa VARCHAR,
    p_descripcion_tarifa VARCHAR,
    p_bloque VARCHAR,
    p_tarifa_promedio DOUBLE PRECISION,
    p_tarifa DOUBLE PRECISION,
    p_pliego VARCHAR,
    p_estructura_costos VARCHAR,
    p_numero_expediente VARCHAR,
    p_numero_resolucion VARCHAR,
    p_fecha_publicacion DATE
)
RETURNS TABLE(ok BOOLEAN, mensaje TEXT, tabla TEXT, filas_afectadas INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO "Staging".stg_distribucion (
        id_mes, mes, anho, fecha, empresa, tipo_tarifa, descripcion_tarifa,
        bloque, tarifa_promedio, tarifa, pliego, estructura_costos,
        numero_expediente, numero_resolucion, fecha_publicacion
    )
    VALUES (
        p_id_mes, p_mes, p_anho, p_fecha, p_empresa, p_tipo_tarifa, p_descripcion_tarifa,
        p_bloque, p_tarifa_promedio, p_tarifa, p_pliego, p_estructura_costos,
        p_numero_expediente, p_numero_resolucion, p_fecha_publicacion
    );

    RETURN QUERY SELECT TRUE, 'Fila insertada correctamente', 'stg_distribucion', 1;
EXCEPTION
    WHEN OTHERS THEN
        RETURN QUERY SELECT FALSE, SQLERRM, 'stg_distribucion', 0;
END;
$$;

CREATE OR REPLACE FUNCTION "Staging".fn_stg_insert_hidrocarburos(
    p_numero_expediente VARCHAR,
    p_numero_resolucion VARCHAR,
    p_fecha_publicacion DATE,
    p_alcance_gaceta VARCHAR,
    p_numero_gaceta INTEGER,
    p_producto VARCHAR,
    p_tipo_cambio DOUBLE PRECISION,
    p_precio_referencia_internacional DOUBLE PRECISION,
    p_precio_colonizado DOUBLE PRECISION,
    p_otros_ingresos_prorrateados DOUBLE PRECISION,
    p_asig_cruz_pesc DOUBLE PRECISION,
    p_asig_cruz_minae DOUBLE PRECISION,
    p_sub_cruz_pesc DOUBLE PRECISION,
    p_sub_cruz_minae DOUBLE PRECISION,
    p_diferencial_precios DOUBLE PRECISION,
    p_impuesto_unico DOUBLE PRECISION,
    p_canon DOUBLE PRECISION,
    p_margen_operacion DOUBLE PRECISION,
    p_rend_tarif DOUBLE PRECISION,
    p_precio_plantel_sin_impuesto DOUBLE PRECISION,
    p_precio_con_impuesto DOUBLE PRECISION,
    p_margen_estaciones_terrestres DOUBLE PRECISION,
    p_margen_estaciones_aereas DOUBLE PRECISION,
    p_flete_estaciones DOUBLE PRECISION,
    p_margen_envasador DOUBLE PRECISION,
    p_margen_distribuidor DOUBLE PRECISION,
    p_margen_detallista DOUBLE PRECISION,
    p_rige BOOLEAN,
    p_precio_consumidor_final_gas DOUBLE PRECISION,
    p_precio_final DOUBLE PRECISION,
    p_precio_final_sin_punto_fijo DOUBLE PRECISION
)
RETURNS TABLE(ok BOOLEAN, mensaje TEXT, tabla TEXT, filas_afectadas INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO "Staging".stg_hidrocarburos (
        numero_expediente, numero_resolucion, fecha_publicacion, alcance_gaceta,
        numero_gaceta, producto, tipo_cambio, precio_referencia_internacional,
        precio_colonizado, otros_ingresos_prorrateados, asig_cruz_pesc, asig_cruz_minae,
        sub_cruz_pesc, sub_cruz_minae, diferencial_precios, impuesto_unico, canon,
        margen_operacion, rend_tarif, precio_plantel_sin_impuesto, precio_con_impuesto,
        margen_estaciones_terrestres, margen_estaciones_aereas, flete_estaciones,
        margen_envasador, margen_distribuidor, margen_detallista, rige,
        precio_consumidor_final_gas, precio_final, precio_final_sin_punto_fijo
    )
    VALUES (
        p_numero_expediente, p_numero_resolucion, p_fecha_publicacion, p_alcance_gaceta,
        p_numero_gaceta, p_producto, p_tipo_cambio, p_precio_referencia_internacional,
        p_precio_colonizado, p_otros_ingresos_prorrateados, p_asig_cruz_pesc, p_asig_cruz_minae,
        p_sub_cruz_pesc, p_sub_cruz_minae, p_diferencial_precios, p_impuesto_unico, p_canon,
        p_margen_operacion, p_rend_tarif, p_precio_plantel_sin_impuesto, p_precio_con_impuesto,
        p_margen_estaciones_terrestres, p_margen_estaciones_aereas, p_flete_estaciones,
        p_margen_envasador, p_margen_distribuidor, p_margen_detallista, p_rige,
        p_precio_consumidor_final_gas, p_precio_final, p_precio_final_sin_punto_fijo
    );

    RETURN QUERY SELECT TRUE, 'Fila insertada correctamente', 'stg_hidrocarburos', 1;
EXCEPTION
    WHEN OTHERS THEN
        RETURN QUERY SELECT FALSE, SQLERRM, 'stg_hidrocarburos', 0;
END;
$$;
