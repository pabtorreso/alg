WITH registros AS (
    SELECT
        equipo_codigo,
        fecha_inicio,
        tipo_reg
    FROM (
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_anglo_export
        UNION ALL
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_catodo_export
        UNION ALL
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_cgo_andina_export
        UNION ALL
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_cgo_cumet_ventanas_export
        UNION ALL
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_cucons_export
        UNION ALL
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_eteo_export
        UNION ALL
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_kdm_export
        UNION ALL
        SELECT equipo_codigo, fecha_inicio, 'turno' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_registro_diario_spot_export
        UNION ALL
        SELECT codigo_interno AS equipo_codigo, fecha AS fecha_inicio, 'notificacion' AS tipo_reg
        FROM CONSULTAS_CGO_EXT.v_notificacion_reporte
        WHERE motivo = 'FALLA - DAÑO'
    ) AS r
),
-- Ordenar y crear grupos según el último turno visto
agrupado AS (
    SELECT 
        equipo_codigo,
        fecha_inicio,
        tipo_reg,
        SUM(CASE WHEN tipo_reg = 'turno' THEN 1 ELSE 0 END) 
            OVER (PARTITION BY equipo_codigo ORDER BY fecha_inicio ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS grupo_turno
    FROM registros
),
-- Tomar la primera notificación de cada bloque antes de un turno
bloques_falla AS (
    SELECT 
        equipo_codigo,
        MIN(fecha_inicio) AS fecha_falla,
        MIN(fecha_turno_siguiente) AS fecha_reanudacion
    FROM (
        SELECT 
            a.equipo_codigo,
            a.grupo_turno,
            a.fecha_inicio,
            a.tipo_reg,
            (SELECT MIN(b.fecha_inicio)
             FROM agrupado b
             WHERE b.equipo_codigo = a.equipo_codigo
               AND b.tipo_reg = 'turno'
               AND b.fecha_inicio > a.fecha_inicio) AS fecha_turno_siguiente
        FROM agrupado a
        WHERE a.tipo_reg = 'notificacion'
    ) x
    GROUP BY equipo_codigo, grupo_turno
),
-- Calcular días fuera (solo si hay turno posterior)
diferencias AS (
    SELECT 
        equipo_codigo,
        fecha_falla,
        fecha_reanudacion,
        EXTRACT(EPOCH FROM (fecha_reanudacion - fecha_falla)) / 86400 AS dias_fuera_servicio
    FROM bloques_falla
    WHERE fecha_reanudacion IS NOT NULL
)
-- Promedio y total por equipo
SELECT 
    equipo_codigo,
    COUNT(*) AS total_periodos_fuera_servicio,
    ROUND(CAST(AVG(dias_fuera_servicio) AS numeric), 2) AS promedio_dias_fuera_servicio
FROM diferencias
GROUP BY equipo_codigo
ORDER BY equipo_codigo;
