-- ============================================================
-- POBLACION DE REPROGRAMACION_OTM
-- ============================================================

-- Insertar con n_reprogramacion calculado ANTES del JOIN
WITH reprogramaciones_numeradas AS (
    SELECT 
        tr.otm_numero,
        tr.reg_fecha_inicio_real::date AS fecha_inicio,
        tr.reprogramaciones_motivo,
        ROW_NUMBER() OVER (
            PARTITION BY tr.otm_numero 
            ORDER BY tr.reg_fecha_inicio_real
        ) AS n_reprogramacion
    FROM temp_reprogramacion tr
    WHERE tr.reg_fecha_inicio_real IS NOT NULL
),
orden_man_sin_duplicados AS (
    SELECT DISTINCT ON (otm_desc)
        otm_id,
        otm_desc
    FROM ORDEN_MAN
)
INSERT INTO REPROGRAMACION_OTM (n_reprogramacion, otm_id, fecha_inicio, motivo_reprogramacion_id)
SELECT 
    rn.n_reprogramacion,
    om.otm_id,
    rn.fecha_inicio,
    mr.motivo_reprogramacion_id
FROM reprogramaciones_numeradas rn
INNER JOIN orden_man_sin_duplicados om 
    ON rn.otm_numero = om.otm_desc
INNER JOIN MOTIVO_REPROGRAMACION mr 
    ON rn.reprogramaciones_motivo = mr.motivo_reprogramacion_desc
WHERE NOT EXISTS (
    SELECT 1 
    FROM REPROGRAMACION_OTM r 
    WHERE r.n_reprogramacion = rn.n_reprogramacion 
      AND r.otm_id = om.otm_id
)
ORDER BY om.otm_id, rn.n_reprogramacion;