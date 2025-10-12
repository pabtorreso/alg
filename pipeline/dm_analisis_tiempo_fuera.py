import pandas as pd
import numpy as np

def procesar_tiempo_fuera(df):
    """Procesa el DataFrame de tiempo fuera de servicio"""
    print("\n=== PROCESANDO TIEMPO FUERA DE SERVICIO ===\n")
    
    # Limpiar NULLs
    df = df.replace(['NULL', '', 'None'], np.nan)
    
    # Convertir tipos
    df['total_periodos_fuera_servicio'] = pd.to_numeric(df['total_periodos_fuera_servicio'], errors='coerce')
    df['promedio_dias_fuera_servicio'] = pd.to_numeric(df['promedio_dias_fuera_servicio'], errors='coerce')
    
    # Eliminar filas sin equipo
    df = df[df['equipo_codigo'].notna()]
    
    print(f"Equipos con historial de tiempo fuera: {len(df)}")
    print(f"Promedio general días fuera: {df['promedio_dias_fuera_servicio'].mean():.2f}")
    print(f"\n✓ Procesado: {len(df)} filas")
    
    return df