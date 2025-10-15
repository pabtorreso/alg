import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import warnings
import os
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")

# ============================================================
# 🔧 CONEXIONES
# ============================================================

DB_ORIGEN = f"dbname={os.getenv('DB_ORIGEN_NAME')} user={os.getenv('DB_ORIGEN_USER')} password='{os.getenv('DB_ORIGEN_PASSWORD')}' host={os.getenv('DB_ORIGEN_HOST')} sslmode=disable"
DB_DESTINO = f"dbname={os.getenv('DB_DESTINO_NAME')} user={os.getenv('DB_DESTINO_USER')} password='{os.getenv('DB_DESTINO_PASSWORD')}' host={os.getenv('DB_DESTINO_HOST')} port={os.getenv('DB_DESTINO_PORT')} sslmode=disable"

# ============================================================
# 📥 QUERY DE EXTRACCIÓN
# ============================================================

QUERY_ORIGEN = """
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

# ============================================================
# 🧠 LIMPIEZA + IMPUTACIÓN ESTADÍSTICA
# ============================================================

def limpiar_motivos(df):
    """Limpia textos y normaliza valores nulos."""
    df['motivo_reprogramacion_desc'] = df['motivo_reprogramacion_desc'].fillna('').str.strip()
    df['motivo_reprogramacion_desc'] = df['motivo_reprogramacion_desc'].replace('', None)
    return df


def imputar_motivos_estadisticos(df):
    """Imputa los motivos faltantes basándose en patrones históricos."""
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

# ============================================================
# 📄 CARGA CON TRUNCADO PREVIO
# ============================================================

def recargar_tablas(conn_dest, df):
    """Trunca y repuebla las tablas con los nuevos datos."""
    cur = conn_dest.cursor()

    print("🧹 Limpiando tablas de destino...")
    cur.execute("TRUNCATE TABLE public.reprogramacion_otm RESTART IDENTITY CASCADE;")
    cur.execute("TRUNCATE TABLE public.motivo_reprogramacion RESTART IDENTITY CASCADE;")
    conn_dest.commit()
    print("✅ Tablas truncadas correctamente.")

    motivos_unicos = df['motivo_reprogramacion_desc'].dropna().unique()
    execute_values(
        cur,
        "INSERT INTO public.motivo_reprogramacion (motivo_reprogramacion_desc) VALUES %s;",
        [(str(m),) for m in motivos_unicos]
    )
    conn_dest.commit()
    print(f"✅ {len(motivos_unicos)} motivos válidos insertados en motivo_reprogramacion.")

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

    print(f"✅ {len(registros)} registros insertados en reprogramacion_otm (recarga total).")

# ============================================================
# 🚀 MAIN
# ============================================================

def ejecutar_proceso():
    print("\n" + "="*60)
    print("🚀 PIPELINE: REPROGRAMACIONES")
    print("="*60)

    with psycopg2.connect(DB_ORIGEN) as conn_origen, psycopg2.connect(DB_DESTINO) as conn_dest:
        df = pd.read_sql_query(QUERY_ORIGEN, conn_origen)
        print(f"📦 Registros extraídos: {len(df)}")

        df = limpiar_motivos(df)
        df = imputar_motivos_estadisticos(df)

        df_final = df.dropna(subset=['motivo_reprogramacion_desc']).copy()
        perdidos = len(df) - len(df_final)

        recargar_tablas(conn_dest, df_final)

        print(f"📊 Registros sin motivo no imputado: {perdidos}")

    print("✅ Pipeline de reprogramaciones completado.\n")

# ============================================================
# 🔥 EJECUCIÓN
# ============================================================

if __name__ == "__main__":
    ejecutar_proceso()