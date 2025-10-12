-- POBLAR PROXIMO_MANTENIMIENTO
INSERT INTO PROXIMO_MANTENIMIENTO (
    equipo_id,
    faena_id,
    ultimo_horometro_otm,
    fec_ultima_otm,
    prom_horas_entre_otm,
    prom_horas_trabajadas_diarias,
    dias_restantes,
    fecha_prox_otm,
    horometro_prox_otm
)
SELECT DISTINCT
    eq.equipo_id,
    f.faena_id,
    tp.horometro_ultimo_mantenimiento::numeric(8,0),
    tp.fecha_ultimo_mantenimiento::date,
    tp.promedio_horas_entre_mantenimientos::numeric(5,0),
    tp.promedio_horas_trabajadas_diarias::numeric(5,0),
    tp.dias_restantes_aprox::numeric(3,1),
    tp.fecha_proximo_mantenimiento::date,
    tp.horometro_estimado_proximo_mantenimiento::numeric(7,0)
FROM temp_prox_mantenimiento tp
JOIN EQUIPO eq ON eq.equipo_desc = tp.equipo_codigo
JOIN FAENA f ON f.faena_desc = tp.faena
WHERE tp.equipo_codigo IS NOT NULL
ON CONFLICT (equipo_id) DO UPDATE SET
    faena_id = EXCLUDED.faena_id,
    ultimo_horometro_otm = EXCLUDED.ultimo_horometro_otm,
    fec_ultima_otm = EXCLUDED.fec_ultima_otm,
    prom_horas_entre_otm = EXCLUDED.prom_horas_entre_otm,
    prom_horas_trabajadas_diarias = EXCLUDED.prom_horas_trabajadas_diarias,
    dias_restantes = EXCLUDED.dias_restantes,
    fecha_prox_otm = EXCLUDED.fecha_prox_otm,
    horometro_prox_otm = EXCLUDED.horometro_prox_otm;

-- POBLAR TIEMPO_BAJA (requiere procesamiento adicional)
-- Esta tabla necesita registros individuales por fecha, no resumen
-- Se omite porque el CSV solo tiene agregados, no detalle por fecha