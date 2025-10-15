"""
Pipeline ETL para limpieza y carga de datos
Ejecuta los servicios de reprogramación y compras en orden
"""

import sys
from datetime import datetime
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import re
import warnings

load_dotenv()
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")

# ============================================================
# CONEXIONES
# ============================================================

DB_ORIGEN = f"dbname={os.getenv('DB_ORIGEN_NAME')} user={os.getenv('DB_ORIGEN_USER')} password='{os.getenv('DB_ORIGEN_PASSWORD')}' host={os.getenv('DB_ORIGEN_HOST')} sslmode=disable"
DB_DESTINO = f"dbname={os.getenv('DB_DESTINO_NAME')} user={os.getenv('DB_DESTINO_USER')} password='{os.getenv('DB_DESTINO_PASSWORD')}' host={os.getenv('DB_DESTINO_HOST')} port={os.getenv('DB_DESTINO_PORT')} sslmode=disable"

# ============================================================
# QUERIES DE EXTRACCIÓN
# ============================================================

QUERY_REPROGRAMACION = """
WITH ordenes AS (
    SELECT 
        nombre_faena,
        codigo_interno AS equipo_desc,
        actividad,
        estado_actividad,
        fecha_original,
        numero_otm AS otm_desc,
        fecha_inicio,
        motivo_no_cumplimiento AS motivo_reprogramacion_desc,
        ROW_NUMBER() OVER (PARTITION BY numero_otm ORDER BY fecha_inicio ASC) AS rn
    FROM consultas_cgo_ext.v_reg_historico_ot_orden
    WHERE numero_otm LIKE 'M%'
)
SELECT 
    nombre_faena,
    equipo_desc,
    actividad,
    estado_actividad,
    fecha_original,
    otm_desc,
    fecha_inicio,
    motivo_reprogramacion_desc
FROM ordenes
WHERE rn > 1
ORDER BY otm_desc, fecha_inicio;
"""

QUERY_COMPRAS = """
WITH ot_mantenimiento AS (
    SELECT
        motivo_compra,
        item_material_o_servicio
    FROM consultas_cgo_ext.v_sol_items_otm_otr
    WHERE motivo_compra IS NOT NULL 
       OR item_material_o_servicio IS NOT NULL
)
SELECT DISTINCT
    motivo_compra,
    item_material_o_servicio
FROM ot_mantenimiento;
"""

# ============================================================
# LIMPIEZA - COMPRAS
# ============================================================

ABREVIACIONES = {
    'hrs': 'horas', 'mtto': 'mantenimiento', 'mant': 'mantenimiento',
    'rep': 'reparacion', 'serv': 'servicio', 'equip': 'equipo',
    'flex': 'flexible', 'fabr': 'fabricacion', 'trasl': 'traslado',
    'comb': 'combustible', 'lubr': 'lubricante', 'filt': 'filtro',
    'camb': 'cambio', 'inst': 'instalacion', 'sist': 'sistema',
}

STOPWORDS = {
    'para', 'desde', 'por', 'con', 'sin', 'del', 'de', 'la', 'el', 
    'los', 'las', 'una', 'uno', 'y', 'o', 'en', 'a', 'al', 'un',
    'otl', 'otm', 'otr', 'ot', 'solicitud', 'hijuelas', 'ventanas',
    'tortolas', 'kdm', 'faena', 'unidad', 'dias', 'horas'
}

TERMINOS_MOTIVOS = {
    'mantenimiento', 'reparacion', 'cambio', 'instalacion', 'fabricacion',
    'servicio', 'traslado', 'inspeccion', 'revision', 'limpieza',
    'lavado', 'pintura', 'soldadura', 'calibracion', 'ajuste',
    'cotizacion', 'compra', 'reemplazo', 'desmontaje', 'montaje',
    'certificacion', 'prueba', 'diagnostico', 'evaluacion'
}

TERMINOS_ITEMS = {
    'filtro', 'aceite', 'combustible', 'flexible', 'neumatico',
    'kit', 'repuesto', 'componente', 'sistema', 'motor', 'freno',
    'cabina', 'vidrio', 'lubricante', 'sello', 'bomba', 'valvula',
    'sensor', 'manguera', 'correa', 'bateria', 'alternador',
    'acumulador', 'adaptador', 'abrazadera', 'conexion', 'empaque',
    'junta', 'perno', 'tuerca', 'tornillo', 'placa', 'lamina',
    'rodamiento', 'retenes', 'cojinete', 'eje', 'engrane',
    'cilindro', 'piston', 'biela', 'carburador', 'inyector',
    'turbo', 'radiador', 'ventilador', 'compresor', 'condensador',
    'evaporador', 'amortiguador', 'suspension', 'resorte', 'barra',
    'varilla', 'vastago', 'bulon', 'pasador', 'chaveta',
    'polea', 'tensor', 'damper', 'volante', 'embrague',
    'clutch', 'transmision', 'diferencial', 'corona', 'pinon',
    'cadena', 'oruga', 'zapata', 'rodillo', 'buje', 'casquillo',
    'espaciador', 'arandela', 'retenedor', 'guardapolvo', 'fuelle',
    'tapa', 'cubierta', 'protector', 'relay', 'fusible',
    'contactor', 'interruptor', 'switch', 'caudalimetro',
    'manometro', 'termometro', 'tacometro', 'indicador',
    'medidor', 'transmisor', 'receptor', 'llave', 'broca',
    'disco', 'esmeril', 'soldador', 'electrodo', 'alambre'
}


def normalizar_texto(texto):
    if pd.isna(texto) or texto == '':
        return None
    
    texto = str(texto).lower().strip()
    
    if re.match(r'^\d{6,}[a-z]?$', texto):
        return None
    
    texto = texto.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    texto = re.sub(r'\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b', '', texto)
    texto = re.sub(r'\b[a-z]{2}-\d{1,3}\b', '', texto)
    texto = re.sub(r'\b[mr]\d{7}\b', '', texto)
    texto = re.sub(r'^\s*-?\d+\s+', '', texto)
    texto = re.sub(r'^-\s*', '', texto)
    
    if re.match(r'^-?\d+k?m?$', texto.strip()):
        return None
    
    for a, s in {'á':'a','é':'e','í':'i','ó':'o','ú':'u','ñ':'n'}.items():
        texto = texto.replace(a, s)
    
    texto = re.sub(r'[^\w\s-]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    
    if len(texto) < 3 or re.match(r'^[a-z]\s+[a-z]$', texto):
        return None
    
    return texto if texto else None


def expandir_abreviaciones(texto):
    if not texto:
        return None
    palabras = [ABREVIACIONES.get(p, p) for p in texto.split()]
    return ' '.join(palabras)


def extraer_concepto_principal(texto, diccionario_terminos):
    if not texto:
        return None
    
    for palabra in texto.split():
        if palabra in diccionario_terminos:
            return palabra
    
    return None


def estandarizar_motivo(texto, max_length=50):
    if pd.isna(texto) or texto == '':
        return None
    
    texto = normalizar_texto(texto)
    if not texto:
        return None
    
    texto = expandir_abreviaciones(texto)
    if not texto:
        return None
    
    concepto = extraer_concepto_principal(texto, TERMINOS_MOTIVOS)
    if not concepto:
        return None
    
    return concepto.strip() if len(concepto.strip()) >= 3 else None


def estandarizar_item(texto, max_length=50):
    if pd.isna(texto) or texto == '':
        return None
    
    texto = normalizar_texto(texto)
    if not texto:
        return None
    
    texto = expandir_abreviaciones(texto)
    if not texto:
        return None
    
    concepto = extraer_concepto_principal(texto, TERMINOS_ITEMS)
    if not concepto:
        return None
    
    return concepto.strip() if len(concepto.strip()) >= 3 else None


def limpiar_motivos_items(df):
    print("Extrayendo conceptos tecnicos...")
    
    total_motivos = df['motivo_compra'].notna().sum()
    total_items = df['item_material_o_servicio'].notna().sum()
    
    df['motivo_compra_limpio'] = df['motivo_compra'].apply(estandarizar_motivo)
    df['item_limpio'] = df['item_material_o_servicio'].apply(estandarizar_item)
    
    motivos_validos = df['motivo_compra_limpio'].notna().sum()
    items_validos = df['item_limpio'].notna().sum()
    
    print(f"   Motivos: {total_motivos} -> {motivos_validos} ({total_motivos - motivos_validos} descartados)")
    print(f"   Items: {total_items} -> {items_validos} ({total_items - items_validos} descartados)")
    
    return df


def deduplicar_catalogos(df, columna):
    unicos = sorted(set(df[columna].dropna().unique().tolist()))
    print(f"   Total conceptos unicos: {len(unicos)}")
    return unicos


def recargar_tablas_compras(conn_dest, motivos, items):
    cur = conn_dest.cursor()

    print("Limpiando tablas de catalogo...")
    cur.execute("TRUNCATE TABLE public.motivo_compra RESTART IDENTITY CASCADE;")
    cur.execute("TRUNCATE TABLE public.item RESTART IDENTITY CASCADE;")
    conn_dest.commit()

    if motivos:
        execute_values(cur, "INSERT INTO public.motivo_compra (motvo_compra_desc) VALUES %s;", [(m,) for m in motivos])
        conn_dest.commit()
        print(f"{len(motivos)} motivos insertados.")
    
    if items:
        execute_values(cur, "INSERT INTO public.item (item_desc) VALUES %s;", [(i,) for i in items])
        conn_dest.commit()
        print(f"{len(items)} items insertados.")
    
    cur.close()


# ============================================================
# LIMPIEZA - REPROGRAMACION
# ============================================================

def limpiar_motivos(df):
    df['motivo_reprogramacion_desc'] = df['motivo_reprogramacion_desc'].fillna('').str.strip()
    df['motivo_reprogramacion_desc'] = df['motivo_reprogramacion_desc'].replace('', None)
    return df


def imputar_motivos_estadisticos(df):
    base = df.dropna(subset=['motivo_reprogramacion_desc']).copy()

    moda_actividad = (
        base.groupby('actividad')['motivo_reprogramacion_desc']
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        .to_dict()
    )

    moda_estado = (
        base.groupby('estado_actividad')['motivo_reprogramacion_desc']
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        .to_dict()
    )

    def imputar(row):
        if pd.isna(row['motivo_reprogramacion_desc']):
            act = row.get('actividad')
            est = row.get('estado_actividad')
            if act in moda_actividad and moda_actividad[act] is not None:
                return moda_actividad[act]
            elif est in moda_estado and moda_estado[est] is not None:
                return moda_estado[est]
            else:
                return None
        return row['motivo_reprogramacion_desc']

    df['motivo_reprogramacion_desc'] = df.apply(imputar, axis=1)
    return df


def recargar_tablas_reprogramacion(conn_dest, df):
    cur = conn_dest.cursor()

    print("Limpiando tablas de destino...")
    cur.execute("TRUNCATE TABLE public.reprogramacion_otm RESTART IDENTITY CASCADE;")
    cur.execute("TRUNCATE TABLE public.motivo_reprogramacion RESTART IDENTITY CASCADE;")
    conn_dest.commit()
    print("Tablas truncadas correctamente.")

    motivos_unicos = df['motivo_reprogramacion_desc'].dropna().unique()
    execute_values(
        cur,
        "INSERT INTO public.motivo_reprogramacion (motivo_reprogramacion_desc) VALUES %s;",
        [(str(m),) for m in motivos_unicos]
    )
    conn_dest.commit()
    print(f"{len(motivos_unicos)} motivos validos insertados en motivo_reprogramacion.")

    motivos = pd.read_sql_query(
        "SELECT motivo_reprogramacion_id, motivo_reprogramacion_desc FROM public.motivo_reprogramacion;",
        conn_dest
    )
    otm = pd.read_sql_query(
        "SELECT otm_id, otm_desc FROM public.orden_man;",
        conn_dest
    )

    df = df.merge(motivos, on='motivo_reprogramacion_desc', how='left')
    df = df.merge(otm, on='otm_desc', how='left')
    df = df.dropna(subset=['otm_id', 'motivo_reprogramacion_id'])
    df = df.sort_values(['otm_id', 'fecha_inicio'])
    df['n_reprogramacion'] = df.groupby('otm_id').cumcount() + 1

    registros = [
        (int(r.otm_id), int(r.n_reprogramacion), r.fecha_inicio, int(r.motivo_reprogramacion_id))
        for _, r in df.iterrows()
    ]

    execute_values(
        cur,
        """
        INSERT INTO public.reprogramacion_otm (otm_id, n_reprogramacion, fecha_inicio, motivo_reprogramacion_id)
        VALUES %s;
        """,
        registros
    )
    conn_dest.commit()
    cur.close()

    print(f"{len(registros)} registros insertados en reprogramacion_otm (recarga total).")


# ============================================================
# PROCESOS
# ============================================================

def ejecutar_proceso_reprogramacion():
    print("\n" + "="*60)
    print("PIPELINE: REPROGRAMACIONES")
    print("="*60)

    with psycopg2.connect(DB_ORIGEN) as conn_origen, psycopg2.connect(DB_DESTINO) as conn_dest:
        df = pd.read_sql_query(QUERY_REPROGRAMACION, conn_origen)
        print(f"Registros extraidos: {len(df)}")

        df = limpiar_motivos(df)
        df = imputar_motivos_estadisticos(df)

        df_final = df.dropna(subset=['motivo_reprogramacion_desc']).copy()
        perdidos = len(df) - len(df_final)

        recargar_tablas_reprogramacion(conn_dest, df_final)

        print(f"Registros sin motivo no imputado: {perdidos}")

    print("Pipeline de reprogramaciones completado.\n")


def ejecutar_proceso_compras():
    print("\n" + "="*60)
    print("PIPELINE: COMPRAS (CATALOGOS)")
    print("="*60)

    with psycopg2.connect(DB_ORIGEN) as conn_origen, psycopg2.connect(DB_DESTINO) as conn_dest:
        df = pd.read_sql_query(QUERY_COMPRAS, conn_origen)
        print(f"Registros extraidos: {len(df)}")

        df = limpiar_motivos_items(df)
        
        print("\nMOTIVOS DE COMPRA:")
        motivos_unicos = deduplicar_catalogos(df, 'motivo_compra_limpio')
        
        print("\nITEMS:")
        items_unicos = deduplicar_catalogos(df, 'item_limpio')
        
        print(f"\nCATALOGOS FINALES:")
        print(f"   {len(motivos_unicos)} motivos")
        print(f"   {len(items_unicos)} items")
        
        recargar_tablas_compras(conn_dest, motivos_unicos, items_unicos)

    print("Pipeline completado.\n")


# ============================================================
# MAIN
# ============================================================

def main():
    inicio = datetime.now()
    print("\n" + "="*60)
    print("INICIANDO PIPELINES ETL")
    print("="*60)
    print(f"Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    exitoso = True
    
    # Pipeline 1: Reprogramaciones
    #try:
    #    ejecutar_proceso_reprogramacion()
    #except Exception as e:
    #    print(f"Error en pipeline de reprogramaciones: {e}")
    #    exitoso = False
    
    # Pipeline 2: Compras
    try:
        ejecutar_proceso_compras()
    except Exception as e:
        print(f"Error en pipeline de compras: {e}")
        exitoso = False
    
    # Resumen final
    fin = datetime.now()
    duracion = (fin - inicio).total_seconds()
    
    print("="*60)
    if exitoso:
        print("TODOS LOS PIPELINES COMPLETADOS EXITOSAMENTE")
    else:
        print("PIPELINES COMPLETADOS CON ERRORES")
    print(f"Duracion total: {duracion:.2f} segundos")
    print("="*60 + "\n")
    
    return 0 if exitoso else 1


if __name__ == "__main__":
    sys.exit(main())