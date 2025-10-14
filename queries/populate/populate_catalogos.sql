-- ============================================================
-- POBLACION DE CATALOGOS DESDE DATOS LIMPIOS
-- ============================================================

-- 1. MOTIVO_COMPRA (desde dm_analisis_compras)
INSERT INTO MOTIVO_COMPRA (motvo_compra_desc)
SELECT DISTINCT motivo_compra
FROM temp_compras
WHERE motivo_compra IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM MOTIVO_COMPRA mc 
    WHERE mc.motvo_compra_desc = temp_compras.motivo_compra
);

-- 2. ITEM (desde dm_analisis_compras)
INSERT INTO ITEM (item_desc)
SELECT DISTINCT compra_item
FROM temp_compras
WHERE compra_item IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM ITEM i 
    WHERE i.item_desc = temp_compras.compra_item
);

-- 3. MOTIVO_REPROGRAMACION (desde dm_analisis_reprogramacion)
INSERT INTO MOTIVO_REPROGRAMACION (motivo_reprogramacion_desc)
SELECT DISTINCT reprogramaciones_motivo
FROM temp_reprogramacion
WHERE reprogramaciones_motivo IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM MOTIVO_REPROGRAMACION mr 
    WHERE mr.motivo_reprogramacion_desc = temp_reprogramacion.reprogramaciones_motivo
);