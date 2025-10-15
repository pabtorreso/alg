import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import re
from collections import Counter

load_dotenv()

# ============================================================
# 🔧 CONEXIONES
# ============================================================

DB_ORIGEN = f"dbname={os.getenv('DB_ORIGEN_NAME')} user={os.getenv('DB_ORIGEN_USER')} password='{os.getenv('DB_ORIGEN_PASSWORD')}' host={os.getenv('DB_ORIGEN_HOST')} sslmode=disable"
DB_DESTINO = f"dbname={os.getenv('DB_DESTINO_NAME')} user={os.getenv('DB_DESTINO_USER')} password='{os.getenv('DB_DESTINO_PASSWORD')}' host={os.getenv('DB_DESTINO_HOST')} port={os.getenv('DB_DESTINO_PORT')} sslmode=disable"

# ============================================================
# 📥 QUERY DE EXTRACCIÓN
# ============================================================

QUERY_ORIGEN = """
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
# 🧹 LIMPIEZA - SOLO 1 PALABRA TÉCNICA
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

# Términos para MOTIVOS (acciones/servicios/razones de compra)
TERMINOS_MOTIVOS = {
    'mantenimiento', 'reparacion', 'cambio', 'instalacion', 'fabricacion',
    'servicio', 'traslado', 'inspeccion', 'revision', 'limpieza',
    'lavado', 'pintura', 'soldadura', 'calibracion', 'ajuste',
    'cotizacion', 'compra', 'reemplazo', 'desmontaje', 'montaje',
    'certificacion', 'prueba', 'diagnostico', 'evaluacion'
}

# Términos para ITEMS (componentes/materiales/repuestos)
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
    
    # Rechazar códigos numéricos largos
    if re.match(r'^\d{6,}[a-z]?$', texto):
        return None
    
    # Limpiar
    texto = texto.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    texto = re.sub(r'\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b', '', texto)
    texto = re.sub(r'\b[a-z]{2}-\d{1,3}\b', '', texto)
    texto = re.sub(r'\b[mr]\d{7}\b', '', texto)
    texto = re.sub(r'^\s*-?\d+\s+', '', texto)
    texto = re.sub(r'^-\s*', '', texto)
    
    if re.match(r'^-?\d+k?m?$', texto.strip()):
        return None
    
    # Quitar acentos
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
    """Extrae SOLO la primera palabra técnica del diccionario especificado"""
    if not texto:
        return None
    
    for palabra in texto.split():
        if palabra in diccionario_terminos:
            return palabra
    
    return None


def estandarizar_motivo(texto, max_length=50):
    """Extrae concepto de MOTIVO (acción/servicio)"""
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
    """Extrae concepto de ITEM (componente/material)"""
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
    print("🔧 Extrayendo conceptos técnicos...")
    
    total_motivos = df['motivo_compra'].notna().sum()
    total_items = df['item_material_o_servicio'].notna().sum()
    
    df['motivo_compra_limpio'] = df['motivo_compra'].apply(estandarizar_motivo)
    df['item_limpio'] = df['item_material_o_servicio'].apply(estandarizar_item)
    
    motivos_validos = df['motivo_compra_limpio'].notna().sum()
    items_validos = df['item_limpio'].notna().sum()
    
    print(f"   📊 Motivos: {total_motivos} → {motivos_validos} ({total_motivos - motivos_validos} descartados)")
    print(f"   📊 Items: {total_items} → {items_validos} ({total_items - items_validos} descartados)")
    
    return df


def deduplicar_catalogos(df, columna):
    unicos = sorted(set(df[columna].dropna().unique().tolist()))
    print(f"   ✅ Total conceptos únicos: {len(unicos)}")
    return unicos


def recargar_tablas(conn_dest, motivos, items):
    cur = conn_dest.cursor()

    print("🧹 Limpiando tablas de catálogo...")
    cur.execute("TRUNCATE TABLE public.motivo_compra RESTART IDENTITY CASCADE;")
    cur.execute("TRUNCATE TABLE public.item RESTART IDENTITY CASCADE;")
    conn_dest.commit()

    if motivos:
        execute_values(cur, "INSERT INTO public.motivo_compra (motvo_compra_desc) VALUES %s;", [(m,) for m in motivos])
        conn_dest.commit()
        print(f"✅ {len(motivos)} motivos insertados.")
    
    if items:
        execute_values(cur, "INSERT INTO public.item (item_desc) VALUES %s;", [(i,) for i in items])
        conn_dest.commit()
        print(f"✅ {len(items)} items insertados.")
    
    cur.close()


def ejecutar_proceso():
    print("\n" + "="*60)
    print("🚀 PIPELINE: COMPRAS (CATÁLOGOS)")
    print("="*60)

    with psycopg2.connect(DB_ORIGEN) as conn_origen, psycopg2.connect(DB_DESTINO) as conn_dest:
        df = pd.read_sql_query(QUERY_ORIGEN, conn_origen)
        print(f"📦 Registros extraídos: {len(df)}")

        df = limpiar_motivos_items(df)
        
        print("\n🔍 MOTIVOS DE COMPRA:")
        motivos_unicos = deduplicar_catalogos(df, 'motivo_compra_limpio')
        
        print("\n🔍 ITEMS:")
        items_unicos = deduplicar_catalogos(df, 'item_limpio')
        
        print(f"\n📊 CATÁLOGOS FINALES:")
        print(f"   🎯 {len(motivos_unicos)} motivos")
        print(f"   🎯 {len(items_unicos)} items")
        
        recargar_tablas(conn_dest, motivos_unicos, items_unicos)

    print("✅ Pipeline completado.\n")


if __name__ == "__main__":
    ejecutar_proceso()