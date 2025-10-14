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
        estado_programa,
        nombre_prioridad_otm,
        fecha_ejecucion_otm,
        fecha_hora_inicio,
        fecha_hora_fin
    FROM consultas_cgo_ext.v_programa_otm
),

reg_otm AS (
    SELECT
        id_otm,
        numero_otm,
        nombre_faena,
        codigo_interno,
        actividad,
        tipo_actividad,
        estado_actividad,
        motivo_no_cumplimiento,
        fecha_inicio,
        fecha_original
    FROM consultas_cgo_ext.v_reg_historico_ot_orden
    WHERE numero_otm ~ '^M[0-9]+$'
),

equipo AS (
    SELECT DISTINCT
        equipo_codigo,
        TRIM(REGEXP_REPLACE(SPLIT_PART(equipo, ' - ', 1), '^[A-Z0-9-]+ ', '')) AS tipo_equipo,
        SPLIT_PART(equipo, ' - ', 2) AS marca_equipo,
        SPLIT_PART(equipo, ' - ', 3) AS modelo_equipo
    FROM (
        SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_anglo_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_cgo_andina_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_cgo_cumet_ventanas_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_cucons_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_eteo_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_kdm_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_tc_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_catodo_export
        UNION ALL SELECT equipo_codigo, equipo FROM consultas_cgo_ext.v_registro_diario_spot_export
    ) AS sub
),

base AS (
    SELECT 
        DISTINCT 
        -- Identificación de la OTM
        p.id_programa_otm AS id_programa_otm,
        p.numero_otm AS otm_numero,
        p.codigo_tarea AS otm_codigo_tarea,
        p.nombre_prioridad_otm AS otm_prioridad,
        p.usuario_programacion AS otm_usuario_programador,
        p.disponibilidad_insumos AS otm_disponibilidad_insumos,
        p.instrucciones_especiales AS otm_instrucciones_especiales,

        -- Fechas del programa
        p.fecha_limite AS otm_fecha_limite,
        p.fecha_ejecucion_otm AS otm_fecha_ejecucion,
        p.fecha_hora_inicio AS otm_fecha_hora_inicio,
        p.fecha_hora_fin AS otm_fecha_hora_fin,

        -- Fechas históricas de ejecución real
        r.fecha_inicio AS reg_fecha_inicio_real,
        r.fecha_original AS reg_fecha_programada_original,

        -- Información de reprogramaciones
        p.cantidad_reprogramaciones AS reprogramaciones_cantidad,
        r.motivo_no_cumplimiento AS reprogramaciones_motivo,

        -- Datos del equipo
        p.equipo AS equipo_codigo,
        e.tipo_equipo AS equipo_tipo,
        e.marca_equipo AS equipo_marca,
        e.modelo_equipo AS equipo_modelo,

        -- Contexto operativo
        r.nombre_faena AS faena_nombre,
        r.codigo_interno AS faena_codigo_interno,
        r.actividad AS actividad_nombre,
        r.tipo_actividad AS actividad_tipo,
        r.estado_actividad AS actividad_estado,

        -- Funciones de ventana
        ROW_NUMBER() OVER (PARTITION BY p.numero_otm ORDER BY r.fecha_inicio) AS num_fila,
        COUNT(*) OVER (PARTITION BY p.numero_otm) AS total_filas

    FROM programa p
    JOIN reg_otm r 
        ON p.numero_otm = r.numero_otm
    LEFT JOIN equipo e 
        ON TRIM(p.equipo) = TRIM(e.equipo_codigo)
)

-- 🚫 Filtramos para dejar fuera:
--  - el primer registro de cada OTM (num_fila > 1)
--  - las OTMs que aparecen solo una vez (total_filas > 1)
SELECT 
    id_programa_otm,
    otm_numero,
    otm_codigo_tarea,
    otm_prioridad,
    otm_usuario_programador,
    otm_disponibilidad_insumos,
    otm_instrucciones_especiales,
    otm_fecha_limite,
    otm_fecha_ejecucion,
    otm_fecha_hora_inicio,
    otm_fecha_hora_fin,
    reg_fecha_inicio_real,
    reg_fecha_programada_original,
    reprogramaciones_cantidad,
    reprogramaciones_motivo,
    equipo_codigo,
    equipo_tipo,
    equipo_marca,
    equipo_modelo,
    faena_nombre,
    faena_codigo_interno,
    actividad_nombre,
    actividad_tipo,
    actividad_estado
FROM base
WHERE num_fila > 1
  AND total_filas > 1
ORDER BY 
    id_programa_otm,
    otm_numero,
    reg_fecha_inicio_real;