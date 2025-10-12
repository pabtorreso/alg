-- POBLAR CATÁLOGOS DESDE TABLAS TEMPORALES
-- Se asume que los datos procesados están en tablas temp_*

-- TIPO_EQUIPO
INSERT INTO TIPO_EQUIPO (tipo_equipo, tipo_equipo_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY equipo_tipo),
    equipo_tipo
FROM (
    SELECT DISTINCT equipo_tipo FROM temp_compras WHERE equipo_tipo IS NOT NULL
    UNION
    SELECT DISTINCT equipo_tipo FROM temp_reprogramacion WHERE equipo_tipo IS NOT NULL
) AS tipos
WHERE NOT EXISTS (
    SELECT 1 FROM TIPO_EQUIPO t WHERE t.tipo_equipo_desc = tipos.equipo_tipo
);

-- MARCA
INSERT INTO MARCA (marca_equipo, marca_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY equipo_marca),
    equipo_marca
FROM (
    SELECT DISTINCT equipo_marca FROM temp_compras WHERE equipo_marca IS NOT NULL
    UNION
    SELECT DISTINCT equipo_marca FROM temp_reprogramacion WHERE equipo_marca IS NOT NULL
) AS marcas
WHERE NOT EXISTS (
    SELECT 1 FROM MARCA m WHERE m.marca_desc = marcas.equipo_marca
);

-- MODELO
INSERT INTO MODELO (modelo_equipo, modelo_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY equipo_modelo),
    equipo_modelo
FROM (
    SELECT DISTINCT equipo_modelo FROM temp_compras WHERE equipo_modelo IS NOT NULL
    UNION
    SELECT DISTINCT equipo_modelo FROM temp_reprogramacion WHERE equipo_modelo IS NOT NULL
) AS modelos
WHERE NOT EXISTS (
    SELECT 1 FROM MODELO mo WHERE mo.modelo_desc = modelos.equipo_modelo
);

-- EQUIPO
INSERT INTO EQUIPO (equipo_id, equipo_desc, tipo_equipo_id, marca_equipo, modelo_equipo)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY e.equipo_codigo),
    e.equipo_codigo,
    te.tipo_equipo,
    ma.marca_equipo,
    mo.modelo_equipo
FROM (
    SELECT DISTINCT equipo_codigo, equipo_tipo, equipo_marca, equipo_modelo 
    FROM temp_compras WHERE equipo_codigo IS NOT NULL
    UNION
    SELECT DISTINCT equipo_codigo, equipo_tipo, equipo_marca, equipo_modelo 
    FROM temp_reprogramacion WHERE equipo_codigo IS NOT NULL
) e
LEFT JOIN TIPO_EQUIPO te ON te.tipo_equipo_desc = e.equipo_tipo
LEFT JOIN MARCA ma ON ma.marca_desc = e.equipo_marca
LEFT JOIN MODELO mo ON mo.modelo_desc = e.equipo_modelo
WHERE NOT EXISTS (
    SELECT 1 FROM EQUIPO eq WHERE eq.equipo_desc = e.equipo_codigo
);

-- FAENA
INSERT INTO FAENA (faena_id, faena_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY faena),
    faena
FROM (
    SELECT DISTINCT faena FROM temp_prox_mantenimiento WHERE faena IS NOT NULL
) f
WHERE NOT EXISTS (
    SELECT 1 FROM FAENA fa WHERE fa.faena_desc = f.faena
);

-- PROVEEDOR
INSERT INTO PROVEEDOR (proveedor_id, proveedor_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY nombre_proveedor),
    nombre_proveedor
FROM temp_compras
WHERE nombre_proveedor IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM PROVEEDOR p WHERE p.proveedor_desc = temp_compras.nombre_proveedor
);

-- CUENTA
INSERT INTO CUENTA (cuenta_id, cuenta_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY descripcion_cuenta),
    descripcion_cuenta
FROM temp_compras
WHERE descripcion_cuenta IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM CUENTA c WHERE c.cuenta_desc = temp_compras.descripcion_cuenta
);

-- CENTRO_COSTO
INSERT INTO CENTRO_COSTO (centro_costo_id, centro_costo_desc, cuenta_id)
SELECT DISTINCT 
    tc.compra_centro_costos,
    tc.compra_centro_costos::varchar,
    cu.cuenta_id
FROM temp_compras tc
LEFT JOIN CUENTA cu ON cu.cuenta_desc = tc.descripcion_cuenta
WHERE tc.compra_centro_costos IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM CENTRO_COSTO cc WHERE cc.centro_costo_id = tc.compra_centro_costos
);

-- MOTIVO_COMPRA
INSERT INTO MOTIVO_COMPRA (motvo_compra_id, motvo_compra_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY motivo_compra),
    motivo_compra
FROM temp_compras
WHERE motivo_compra IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM MOTIVO_COMPRA mc WHERE mc.motvo_compra_desc = temp_compras.motivo_compra
);

-- ITEM
INSERT INTO ITEM (item_id, item_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY compra_item),
    compra_item
FROM temp_compras
WHERE compra_item IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM ITEM i WHERE i.item_desc = temp_compras.compra_item
);

-- MOTIVO_REPROGRAMACION
INSERT INTO MOTIVO_REPROGRAMACION (motivo_reprogramacion_id, motivo_reprogramacion_desc)
SELECT DISTINCT 
    ROW_NUMBER() OVER (ORDER BY reprogramaciones_motivo),
    reprogramaciones_motivo
FROM temp_reprogramacion
WHERE reprogramaciones_motivo IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM MOTIVO_REPROGRAMACION mr WHERE mr.motivo_reprogramacion_desc = temp_reprogramacion.reprogramaciones_motivo
);