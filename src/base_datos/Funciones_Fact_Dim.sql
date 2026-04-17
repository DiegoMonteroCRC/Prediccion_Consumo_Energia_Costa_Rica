CREATE OR REPLACE FUNCTION "Fact_Dim".fn_cargar_a_fact_dim()
RETURNS TABLE(objeto TEXT, filas_insertadas INTEGER, mensaje TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    WITH fechas AS (
        SELECT MAKE_DATE(ano, mes, 1)::DATE AS fecha
        FROM "Staging".stg_clima_nasa
        WHERE ano IS NOT NULL AND mes BETWEEN 1 AND 12
        UNION
        SELECT MAKE_DATE(ano, mes, 1)::DATE AS fecha
        FROM "Staging".stg_aresep_medios
        WHERE ano IS NOT NULL AND mes BETWEEN 1 AND 12
        UNION
        SELECT COALESCE(fecha, MAKE_DATE(anho, id_mes, 1)::DATE) AS fecha
        FROM "Staging".stg_distribucion
        WHERE anho IS NOT NULL AND id_mes BETWEEN 1 AND 12
        UNION
        SELECT fecha_publicacion
        FROM "Staging".stg_hidrocarburos
        WHERE fecha_publicacion IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".dim_tiempo (
            fecha, anio, mes, dia, trimestre, nombre_mes, anio_mes
        )
        SELECT
            fecha,
            EXTRACT(YEAR FROM fecha)::INTEGER,
            EXTRACT(MONTH FROM fecha)::INTEGER,
            EXTRACT(DAY FROM fecha)::INTEGER,
            EXTRACT(QUARTER FROM fecha)::INTEGER,
            TRIM(TO_CHAR(fecha, 'TMMonth')),
            (EXTRACT(YEAR FROM fecha)::INTEGER * 100) + EXTRACT(MONTH FROM fecha)::INTEGER
        FROM fechas
        WHERE fecha IS NOT NULL
        ON CONFLICT (fecha) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'dim_tiempo', v_count, 'Carga de dimension tiempo completada';

    WITH empresas AS (
        SELECT empresa AS nombre_empresa
        FROM "Staging".stg_clima_nasa
        WHERE empresa IS NOT NULL
        UNION
        SELECT empresa
        FROM "Staging".stg_aresep_medios
        WHERE empresa IS NOT NULL
        UNION
        SELECT empresa
        FROM "Staging".stg_distribucion
        WHERE empresa IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".dim_empresa (nombre_empresa)
        SELECT nombre_empresa
        FROM empresas
        ON CONFLICT (nombre_empresa) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'dim_empresa', v_count, 'Carga de dimension empresa completada';

    WITH tarifas AS (
        SELECT
            'MEDIOS|' || COALESCE(tarifa, '') || '|' || COALESCE(sistema, '') || '|' || COALESCE(trimestral, '') AS tarifa_nk,
            tarifa AS nombre_tarifa,
            NULL::VARCHAR AS tipo_tarifa,
            NULL::VARCHAR AS descripcion_tarifa,
            NULL::VARCHAR AS bloque,
            sistema,
            trimestral AS indicador_trimestral
        FROM "Staging".stg_aresep_medios
        WHERE tarifa IS NOT NULL
        UNION
        SELECT
            'DISTRIBUCION|' || COALESCE(tipo_tarifa, '') || '|' || COALESCE(descripcion_tarifa, '') || '|' ||
            COALESCE(bloque, '') || '|' || COALESCE(tarifa::TEXT, '') || '|' || COALESCE(pliego, '') || '|' ||
            COALESCE(estructura_costos, '') AS tarifa_nk,
            COALESCE(descripcion_tarifa, tipo_tarifa) AS nombre_tarifa,
            tipo_tarifa,
            descripcion_tarifa,
            bloque,
            NULL::VARCHAR AS sistema,
            NULL::VARCHAR AS indicador_trimestral
        FROM "Staging".stg_distribucion
        WHERE tipo_tarifa IS NOT NULL OR descripcion_tarifa IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".dim_tarifa (
            tarifa_nk, nombre_tarifa, tipo_tarifa, descripcion_tarifa, bloque, sistema, indicador_trimestral
        )
        SELECT
            tarifa_nk, nombre_tarifa, tipo_tarifa, descripcion_tarifa, bloque, sistema, indicador_trimestral
        FROM tarifas
        ON CONFLICT (tarifa_nk) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'dim_tarifa', v_count, 'Carga de dimension tarifa completada';

    WITH ubicaciones AS (
        SELECT
            COALESCE(provincia, '') || '|' || COALESCE(canton, '') || '|' || COALESCE(distrito, '') || '|' ||
            COALESCE(codigo_dta::TEXT, '') || '|' || COALESCE(coordenada_x::TEXT, '') || '|' ||
            COALESCE(coordenada_y::TEXT, '') AS ubicacion_nk,
            provincia,
            canton,
            distrito,
            codigo_dta,
            coordenada_x,
            coordenada_y
        FROM "Staging".stg_centro
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".dim_ubicacion (
            ubicacion_nk, provincia, canton, distrito, codigo_dta, coordenada_x, coordenada_y
        )
        SELECT
            ubicacion_nk, provincia, canton, distrito, codigo_dta, coordenada_x, coordenada_y
        FROM ubicaciones
        ON CONFLICT (ubicacion_nk) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'dim_ubicacion', v_count, 'Carga de dimension ubicacion completada';

    WITH centrales AS (
        SELECT
            c.id_objecto,
            c.operador,
            c.central_electrica,
            c.fuente,
            u.ubicacion_key
        FROM "Staging".stg_centro c
        INNER JOIN "Fact_Dim".dim_ubicacion u
            ON u.ubicacion_nk = COALESCE(c.provincia, '') || '|' || COALESCE(c.canton, '') || '|' ||
                                COALESCE(c.distrito, '') || '|' || COALESCE(c.codigo_dta::TEXT, '') || '|' ||
                                COALESCE(c.coordenada_x::TEXT, '') || '|' || COALESCE(c.coordenada_y::TEXT, '')
        WHERE c.id_objecto IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".dim_central_electrica (
            id_objecto, operador, central_electrica, fuente, ubicacion_key
        )
        SELECT
            id_objecto, operador, central_electrica, fuente, ubicacion_key
        FROM centrales
        ON CONFLICT (id_objecto) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'dim_central_electrica', v_count, 'Carga de dimension central electrica completada';

    WITH productos AS (
        SELECT DISTINCT producto
        FROM "Staging".stg_hidrocarburos
        WHERE producto IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".dim_producto_hidrocarburo (producto)
        SELECT producto
        FROM productos
        ON CONFLICT (producto) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'dim_producto_hidrocarburo', v_count, 'Carga de dimension producto completada';

    WITH resoluciones AS (
        SELECT DISTINCT
            COALESCE(numero_expediente, '') || '|' || COALESCE(numero_resolucion, '') || '|' ||
            COALESCE(fecha_publicacion::TEXT, '') AS resolucion_nk,
            numero_expediente,
            numero_resolucion,
            fecha_publicacion,
            alcance_gaceta,
            numero_gaceta,
            rige
        FROM "Staging".stg_hidrocarburos
        WHERE numero_resolucion IS NOT NULL OR numero_expediente IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".dim_resolucion_hidrocarburo (
            resolucion_nk, numero_expediente, numero_resolucion, fecha_publicacion,
            alcance_gaceta, numero_gaceta, rige
        )
        SELECT
            resolucion_nk, numero_expediente, numero_resolucion, fecha_publicacion,
            alcance_gaceta, numero_gaceta, rige
        FROM resoluciones
        ON CONFLICT (resolucion_nk) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'dim_resolucion_hidrocarburo', v_count, 'Carga de dimension resolucion completada';

    WITH clima AS (
        SELECT
            t.tiempo_key,
            e.empresa_key,
            s.t2m,
            s.ws10m,
            s.cloud_amt,
            s.rh2m,
            s.t2m_max,
            s.t2m_min,
            s.cloud_od,
            s.gwetroot,
            s.ts,
            s.prectotcorr,
            s.allsky_sfc_sw_dwn,
            s.ps,
            s.t2mwet,
            s.allsky_sfc_sw_diff,
            s.allsky_sfc_lw_dwn
        FROM "Staging".stg_clima_nasa s
        INNER JOIN "Fact_Dim".dim_tiempo t
            ON t.fecha = MAKE_DATE(s.ano, s.mes, 1)
        INNER JOIN "Fact_Dim".dim_empresa e
            ON e.nombre_empresa = s.empresa
        WHERE s.ano IS NOT NULL AND s.mes BETWEEN 1 AND 12 AND s.empresa IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".fact_clima_mensual (
            tiempo_key, empresa_key, t2m, ws10m, cloud_amt, rh2m, t2m_max, t2m_min,
            cloud_od, gwetroot, ts, prectotcorr, allsky_sfc_sw_dwn, ps, t2mwet,
            allsky_sfc_sw_diff, allsky_sfc_lw_dwn
        )
        SELECT
            tiempo_key, empresa_key, t2m, ws10m, cloud_amt, rh2m, t2m_max, t2m_min,
            cloud_od, gwetroot, ts, prectotcorr, allsky_sfc_sw_dwn, ps, t2mwet,
            allsky_sfc_sw_diff, allsky_sfc_lw_dwn
        FROM clima
        ON CONFLICT (tiempo_key, empresa_key) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'fact_clima_mensual', v_count, 'Carga de hecho clima completada';

    WITH tarifas AS (
        SELECT
            t.tiempo_key,
            e.empresa_key,
            d.tarifa_key,
            s.abonados,
            s.ventas,
            s.ingreso_sin_cvg,
            s.ingreso_con_cvg,
            s.precio_medio_sin_cvg,
            s.precio_medio_con_cvg
        FROM "Staging".stg_aresep_medios s
        INNER JOIN "Fact_Dim".dim_tiempo t
            ON t.fecha = MAKE_DATE(s.ano, s.mes, 1)
        INNER JOIN "Fact_Dim".dim_empresa e
            ON e.nombre_empresa = s.empresa
        INNER JOIN "Fact_Dim".dim_tarifa d
            ON d.tarifa_nk = 'MEDIOS|' || COALESCE(s.tarifa, '') || '|' || COALESCE(s.sistema, '') || '|' || COALESCE(s.trimestral, '')
        WHERE s.ano IS NOT NULL AND s.mes BETWEEN 1 AND 12 AND s.empresa IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".fact_tarifa_electricidad (
            tiempo_key, empresa_key, tarifa_key, abonados, ventas, ingreso_sin_cvg,
            ingreso_con_cvg, precio_medio_sin_cvg, precio_medio_con_cvg
        )
        SELECT
            tiempo_key, empresa_key, tarifa_key, abonados, ventas, ingreso_sin_cvg,
            ingreso_con_cvg, precio_medio_sin_cvg, precio_medio_con_cvg
        FROM tarifas
        ON CONFLICT (tiempo_key, empresa_key, tarifa_key) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'fact_tarifa_electricidad', v_count, 'Carga de hecho tarifa electricidad completada';

    WITH distribucion AS (
        SELECT
            t.tiempo_key,
            e.empresa_key,
            d.tarifa_key,
            s.tarifa_promedio
        FROM "Staging".stg_distribucion s
        INNER JOIN "Fact_Dim".dim_tiempo t
            ON t.fecha = COALESCE(s.fecha, MAKE_DATE(s.anho, s.id_mes, 1))
        INNER JOIN "Fact_Dim".dim_empresa e
            ON e.nombre_empresa = s.empresa
        INNER JOIN "Fact_Dim".dim_tarifa d
            ON d.tarifa_nk = 'DISTRIBUCION|' || COALESCE(s.tipo_tarifa, '') || '|' ||
                             COALESCE(s.descripcion_tarifa, '') || '|' || COALESCE(s.bloque, '') || '|' ||
                             COALESCE(s.tarifa::TEXT, '') || '|' || COALESCE(s.pliego, '') || '|' ||
                             COALESCE(s.estructura_costos, '')
        WHERE s.anho IS NOT NULL AND s.id_mes BETWEEN 1 AND 12 AND s.empresa IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".fact_distribucion_tarifaria (
            tiempo_key, empresa_key, tarifa_key, tarifa_promedio
        )
        SELECT
            tiempo_key, empresa_key, tarifa_key, tarifa_promedio
        FROM distribucion
        ON CONFLICT (tiempo_key, empresa_key, tarifa_key) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'fact_distribucion_tarifaria', v_count, 'Carga de hecho distribucion completada';

    WITH hidrocarburos AS (
        SELECT
            t.tiempo_key,
            p.producto_key,
            r.resolucion_key,
            s.tipo_cambio,
            s.precio_referencia_internacional,
            s.precio_colonizado,
            s.otros_ingresos_prorrateados,
            s.asig_cruz_pesc,
            s.asig_cruz_minae,
            s.sub_cruz_pesc,
            s.sub_cruz_minae,
            s.diferencial_precios,
            s.impuesto_unico,
            s.canon,
            s.margen_operacion,
            s.rend_tarif,
            s.precio_plantel_sin_impuesto,
            s.precio_con_impuesto,
            s.margen_estaciones_terrestres,
            s.margen_estaciones_aereas,
            s.flete_estaciones,
            s.margen_envasador,
            s.margen_distribuidor,
            s.margen_detallista,
            s.precio_consumidor_final_gas,
            s.precio_final,
            s.precio_final_sin_punto_fijo
        FROM "Staging".stg_hidrocarburos s
        INNER JOIN "Fact_Dim".dim_tiempo t
            ON t.fecha = s.fecha_publicacion
        INNER JOIN "Fact_Dim".dim_producto_hidrocarburo p
            ON p.producto = s.producto
        INNER JOIN "Fact_Dim".dim_resolucion_hidrocarburo r
            ON r.resolucion_nk = COALESCE(s.numero_expediente, '') || '|' ||
                                 COALESCE(s.numero_resolucion, '') || '|' ||
                                 COALESCE(s.fecha_publicacion::TEXT, '')
        WHERE s.fecha_publicacion IS NOT NULL AND s.producto IS NOT NULL
    ),
    inserted AS (
        INSERT INTO "Fact_Dim".fact_hidrocarburos (
            tiempo_key, producto_key, resolucion_key, tipo_cambio,
            precio_referencia_internacional, precio_colonizado, otros_ingresos_prorrateados,
            asig_cruz_pesc, asig_cruz_minae, sub_cruz_pesc, sub_cruz_minae, diferencial_precios,
            impuesto_unico, canon, margen_operacion, rend_tarif, precio_plantel_sin_impuesto,
            precio_con_impuesto, margen_estaciones_terrestres, margen_estaciones_aereas,
            flete_estaciones, margen_envasador, margen_distribuidor, margen_detallista,
            precio_consumidor_final_gas, precio_final, precio_final_sin_punto_fijo
        )
        SELECT
            tiempo_key, producto_key, resolucion_key, tipo_cambio,
            precio_referencia_internacional, precio_colonizado, otros_ingresos_prorrateados,
            asig_cruz_pesc, asig_cruz_minae, sub_cruz_pesc, sub_cruz_minae, diferencial_precios,
            impuesto_unico, canon, margen_operacion, rend_tarif, precio_plantel_sin_impuesto,
            precio_con_impuesto, margen_estaciones_terrestres, margen_estaciones_aereas,
            flete_estaciones, margen_envasador, margen_distribuidor, margen_detallista,
            precio_consumidor_final_gas, precio_final, precio_final_sin_punto_fijo
        FROM hidrocarburos
        ON CONFLICT (tiempo_key, producto_key, resolucion_key) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_count FROM inserted;
    RETURN QUERY SELECT 'fact_hidrocarburos', v_count, 'Carga de hecho hidrocarburos completada';
END;
$$;
