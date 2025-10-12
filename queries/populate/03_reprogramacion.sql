-- POBLAR REPROGRAMACION_OTM
INSERT INTO REPROGRAMACION_OTM (
    n_reprogramacion,
    otm_id,
    fecha_inicio,
    motivo_reprogramacion_id
)
SELECT DISTINCT
    tr.reprogramaciones_cantidad,
    tr.id_programa_otm,
    tr.reg_fecha_inicio_real::date,
    mr.motivo_reprogramacion_id
FROM temp_reprogramacion tr
JOIN MOTIVO_REPROGRAMACION mr ON mr.motivo_reprogramacion_desc = tr.reprogramaciones_motivo
WHERE tr.reprogramaciones_cantidad > 0
AND tr.reg_fecha_inicio_real IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM REPROGRAMACION_OTM ro 
    WHERE ro.n_reprogramacion = tr.reprogramaciones_cantidad 
    AND ro.otm_id = tr.id_programa_otm
);