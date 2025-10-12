import pandas as pd
import numpy as np

def procesar_proximo_mantenimiento(df):
    """Procesa el DataFrame de próximo mantenimiento"""
    print("\n=== PROCESANDO PRÓXIMO MANTENIMIENTO ===\n")
    
    # Limpiar NULLs y valores vacíos
    df = df.replace(['NULL', '', 'None'], np.nan)
    
    # Convertir tipos
    df['dias_restantes_aprox'] = pd.to_numeric(df['dias_restantes_aprox'], errors='coerce')
    df['promedio_horas_trabajadas_diarias'] = pd.to_numeric(df['promedio_horas_trabajadas_diarias'], errors='coerce')
    
    # Eliminar filas sin equipo
    df = df[df['equipo_codigo'].notna()]
    
    print(f"Equipos con próximo mantenimiento: {len(df)}")
    print(f"Faenas involucradas: {df['faena'].nunique()}")
    print(f"\n✓ Procesado: {len(df)} filas")
    
    return df