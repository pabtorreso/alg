-- POBLAR PROGRAMA (sin oc_id)
INSERT INTO PROGRAMA (
    programa_id,
    equipo_id,
    horometro_referencia,
    disponibilidad_insumos,
    usuario_programacion,
    estado_otm
)
SELECT DISTINCT
    tc.id_programa_otm,
    eq.equipo_id,
    tc.equipo_horometro_planificacion::decimal(5,0),
    'SI', -- Por defecto, ajustar según lógica
    'SISTEMA',
    tc.estado_programa
FROM temp_compras tc
JOIN EQUIPO eq ON eq.equipo_desc = tc.equipo_codigo
WHERE tc.id_programa_otm IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM PROGRAMA pr WHERE pr.programa_id = tc.id_programa_otm
);

-- POBLAR ORDEN_MAN (sin oc_id, solicitud_id en NULL por falta de datos)
INSERT INTO ORDEN_MAN (
    otm_id,
    otm_desc,
    programa_id,
    solicitud_id
)
SELECT DISTINCT
    tc.id_programa_otm,
    tc.otm_numero,
    tc.id_programa_otm,
    NULL  -- No tenemos datos de SOLICITUD/NOTIFICACION en los CSV
FROM temp_compras tc
WHERE tc.otm_numero IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM ORDEN_MAN om WHERE om.otm_id = tc.id_programa_otm
);

-- POBLAR ORDEN_COMPRA (con otm_id)
INSERT INTO ORDEN_COMPRA (
    oc_id, 
    oc_desc, 
    motvo_compra_id, 
    item_id, 
    proveedor_id, 
    centro_costo_id,
    oc_item_cantidad,
    oc_item_unidad,
    oc_item_monto_total,
    oc_monto_neto,
    oc_valor_total,
    oc_monto_total_factura,
    oc_fecha_emision_factura,
    otm_id
)
SELECT DISTINCT
    tc.compra_orden::int,
    tc.compra_orden::varchar,
    mc.motvo_compra_id,
    i.item_id,
    p.proveedor_id,
    cc.centro_costo_id,
    tc.compra_cantidad::int,
    tc.compra_unidad,
    tc.compra_monto_item::decimal(8,0),
    tc.compra_monto_neto::decimal(8,0),
    tc.compra_valor_total::decimal(8,0),
    tc.compra_monto_total_factura::decimal(8,0),
    tc.compra_fecha_factura::date,
    tc.id_programa_otm  -- Ahora ORDEN_COMPRA tiene otm_id
FROM temp_compras tc
LEFT JOIN MOTIVO_COMPRA mc ON mc.motvo_compra_desc = tc.motivo_compra
LEFT JOIN ITEM i ON i.item_desc = tc.compra_item
LEFT JOIN PROVEEDOR p ON p.proveedor_desc = tc.nombre_proveedor
LEFT JOIN CENTRO_COSTO cc ON cc.centro_costo_id = tc.compra_centro_costos
WHERE tc.compra_orden IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM ORDEN_COMPRA oc WHERE oc.oc_id = tc.compra_orden::int
);