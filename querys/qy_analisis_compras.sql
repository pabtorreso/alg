WITH programa AS (
    SELECT
        id_programa_otm,
        otm AS numero_otm,
        equipo,
        codigo_tarea,
        horometro_referencia,
        disponibilidad_insumos,
        instrucciones_especiales,
        fecha_limite,
        cantidad_reprogramaciones,
        usuario_programacion,
        nombre_prioridad_otm,
        fecha_ejecucion_otm,
        horometro_planificacion,
        ultimo_horometro,
        fecha_ultimo_horometro,
        usuario_ultimo_horometro,
        fecha_log,
        fecha_hora_inicio,
        fecha_hora_fin,
		estado_programa
    FROM consultas_cgo_ext.v_programa_otm
),

reg_otm AS (
    SELECT
        id_otm,
        fecha_inicio,
        anio,
        nombre_faena,
        codigo_interno,
        actividad,
        tipo_actividad,
        estado_actividad,
        motivo_no_cumplimiento,
        numero_otm,
        fecha_original,
        cantidad_reprogramaciones
    FROM consultas_cgo_ext.v_reg_historico_ot_orden
    WHERE numero_otm ~ '^M[0-9]+$'
),

ot_mantenimiento AS (
    SELECT
        numero_solicitud,
        fecha_solicitud,
        ot,
        tipo_solicitud,
        equipo,
        solicitante,
        estado_solicitud,
        faena,
        SPLIT_PART(cuenta_contable, ':', 1) AS id_cuenta,
    	TRIM(SPLIT_PART(cuenta_contable, ':', 2)) AS descripcion_cuenta,
        centro_costos,
        SPLIT_PART(proveedor_seleccionado, ' - ', 1) AS proveedor_rut,
		SPLIT_PART(proveedor_seleccionado, ' - ', 2) AS proveedor_nombre,
        fecha_cotizacion,
        condicion_pago,
        monto_neto,
        valor_total,
        plazo_entrega,
        motivo_compra,
        orden_compra,
        fecha_orden_compra,
        fecha_emision_factura,
        monto_total_factura,
        item_material_o_servicio,
        item_cantidad,
        SPLIT_PART(item_unidad, ' - ', 2) AS item_unidad,
        item_monto_total,
        estado_recepcion,
        fecha_aceptado,
        fecha_aceptado_gerencia,
        validador,
        validador_gerencia
    FROM consultas_cgo_ext.v_sol_items_otm_otr
),

equipo AS (
    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_anglo_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_cgo_andina_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_cgo_cumet_ventanas_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_cucons_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_eteo_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_kdm_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_tc_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_catodo_export

    UNION ALL

    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo,
		distrito,
		area_de_servicio
    FROM consultas_cgo_ext.v_registro_diario_spot_export
)

SELECT DISTINCT
    --  Identificación de la OTM
    p.id_programa_otm AS id_programa_otm,
    p.numero_otm AS otm_numero,
	

    --  Datos del equipo
    p.equipo AS equipo_codigo,
    e.tipo_equipo AS equipo_tipo,
    e.marca_equipo AS equipo_marca,
    e.modelo_equipo AS equipo_modelo,
    p.horometro_planificacion AS equipo_horometro_planificacion,

    --  Información de compras e insumos
    otm.numero_solicitud AS compra_numero_solicitud,
    otm.fecha_solicitud AS compra_fecha_solicitud,
	otm.proveedor_rut AS rut_proveedor,
    otm.proveedor_nombre AS nombre_proveedor,
	otm.motivo_compra AS motivo_compra,
    otm.item_material_o_servicio AS compra_item,
    otm.item_cantidad AS compra_cantidad,
    otm.item_unidad AS compra_unidad,
    otm.item_monto_total AS compra_monto_item,
    otm.monto_neto AS compra_monto_neto,
    otm.valor_total AS compra_valor_total,
    otm.monto_total_factura AS compra_monto_total_factura,
    otm.fecha_emision_factura AS compra_fecha_factura,
    otm.orden_compra AS compra_orden,
    otm.estado_solicitud AS compra_estado,
    otm.condicion_pago AS compra_condicion_pago,
    otm.id_cuenta AS id_cuenta,
	otm.descripcion_cuenta AS descripcion_cuenta,
    otm.centro_costos AS compra_centro_costos,
	
	p.estado_programa AS estado_programa 

FROM programa p
LEFT JOIN ot_mantenimiento otm 
	ON p.numero_otm = otm.ot
LEFT JOIN equipo e 
    ON TRIM(p.equipo) = TRIM(e.equipo_codigo)
WHERE p.estado_programa LIKE '%Realizado%' AND otm.item_material_o_servicio IS NOT NULL
ORDER BY 
    p.id_programa_otm,
    p.numero_otm,
    otm.fecha_solicitud

