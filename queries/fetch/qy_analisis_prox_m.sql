WITH 
-- promedio de horas entre mantenimientos
promedio_mantencion AS (
    WITH difs AS (
        SELECT DISTINCT
            equipo,
            horometro_planificacion,
            LAG(horometro_planificacion, 1, 0) OVER (
                PARTITION BY equipo 
                ORDER BY horometro_planificacion
            ) AS horometro_anterior
        FROM CONSULTAS_CGO_EXT.V_PROGRAMA_OTM
        WHERE horometro_planificacion IS NOT NULL
        ORDER BY horometro_planificacion
    )
    SELECT
        equipo,
        ROUND(AVG(horometro_planificacion - horometro_anterior), 0) AS promedio_diferencia_horometro
    FROM difs
    WHERE horometro_anterior IS NOT NULL
    GROUP BY equipo
),
-- promedio de horas trabajadas en el último año
promedio_semanal AS (
    WITH ultimo_anio AS (
        SELECT GENERATE_SERIES(
            CURRENT_DATE - interval '1 year',  -- últimos 12 meses exactos
            CURRENT_DATE,
            interval '1 day'
        )::date AS fecha
    ),
    horometros_turnos AS (
        SELECT
            r.FECHA_INICIO::date AS fecha_inicio,
            r.EQUIPO_CODIGO,
			r.distrito,
            MAX(r.HOROMETRO_FIN) AS horometro_fin
        FROM (
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_anglo_export
            UNION ALL
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_catodo_export
            UNION ALL
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_cgo_andina_export
            UNION ALL
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_cgo_cumet_ventanas_export
            UNION ALL
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_cucons_export
            UNION ALL
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_eteo_export
            UNION ALL
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_kdm_export
            UNION ALL
            SELECT * FROM CONSULTAS_CGO_EXT.v_registro_diario_spot_export
        ) r
        JOIN ultimo_anio s 
        ON r.FECHA_INICIO::date = s.fecha
        WHERE r.HOROMETRO_FIN IS NOT NULL 
        GROUP BY r.EQUIPO_CODIGO, r.distrito,r.FECHA_INICIO::date
    ),
    diferencias AS (
        SELECT
            EQUIPO_CODIGO,
            fecha_inicio,
			distrito,
            horometro_fin,
            LAG(horometro_fin) OVER (
                PARTITION BY EQUIPO_CODIGO 
                ORDER BY fecha_inicio
            ) AS horometro_anterior
        FROM horometros_turnos
    )
    SELECT
        EQUIPO_CODIGO,
		distrito,
        ROUND(AVG(horometro_fin - horometro_anterior), 2) AS promedio_diferencia_horometro
    FROM diferencias
    WHERE horometro_anterior IS NOT NULL
    GROUP BY EQUIPO_CODIGO, distrito
),
-- último horómetro del último mantenimiento
ultimo_mant AS (
    SELECT DISTINCT
        equipo,
        MAX(fecha_ejecucion_otm) AS fecha_mentenimiento,
        MAX(horometro_planificacion) AS horometro_ultimo_mant
    FROM CONSULTAS_CGO_EXT.V_PROGRAMA_OTM
    GROUP BY equipo
)
-- Query final combinada
SELECT
    u.equipo AS equipo_codigo,
	s.distrito AS faena,
    u.horometro_ultimo_mant AS horometro_ultimo_mantenimiento,
    u.fecha_mentenimiento AS fecha_ultimo_mantenimiento,
    p.promedio_diferencia_horometro AS promedio_horas_entre_mantenimientos,
    s.promedio_diferencia_horometro AS promedio_horas_trabajadas_diarias,

    -- Días restantes para el próximo mantenimiento (evita división por cero)
    CASE 
        WHEN COALESCE(s.promedio_diferencia_horometro, 0) = 0 THEN NULL
        ELSE ROUND(p.promedio_diferencia_horometro / s.promedio_diferencia_horometro, 1)
    END AS dias_restantes_aprox,
 
    -- Fecha base estimada del próximo mantenimiento
    CASE 
        WHEN COALESCE(s.promedio_diferencia_horometro, 0) = 0 THEN NULL
        ELSE CURRENT_DATE + (p.promedio_diferencia_horometro / s.promedio_diferencia_horometro) * interval '1 day'
    END AS fecha_base_estimacion,
    
    -- Fecha ajustada para que caiga entre lunes y jueves
    CASE 
        WHEN COALESCE(s.promedio_diferencia_horometro, 0) = 0 THEN NULL
        WHEN EXTRACT(DOW FROM (CURRENT_DATE + (p.promedio_diferencia_horometro / s.promedio_diferencia_horometro) * interval '1 day')) IN (5,6,0)
            THEN date_trunc('week', (CURRENT_DATE + (p.promedio_diferencia_horometro / s.promedio_diferencia_horometro) * interval '1 day') + interval '7 days')::date
        ELSE (CURRENT_DATE + (p.promedio_diferencia_horometro / s.promedio_diferencia_horometro) * interval '1 day')::date
    END AS fecha_proximo_mantenimiento,
    
    (u.horometro_ultimo_mant + p.promedio_diferencia_horometro) AS horometro_estimado_proximo_mantenimiento
FROM ultimo_mant u
JOIN promedio_mantencion p ON p.equipo = u.equipo
JOIN promedio_semanal s ON s.EQUIPO_CODIGO = u.equipo
WHERE COALESCE(s.promedio_diferencia_horometro, 0) > 0
ORDER BY fecha_proximo_mantenimiento ASC;
