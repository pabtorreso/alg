import pandas as pd
import numpy as np

def procesar_notificaciones(df):
    """Procesa el DataFrame de notificaciones"""
    print("\n=== PROCESANDO NOTIFICACIONES ===\n")
    
    # Limpiar NULLs
    df = df.replace(['NULL', '', 'None'], np.nan)
    
    # Llenar valores faltantes
    df['motivo'] = df['motivo'].fillna('Sin Especificar')
    df['sintoma'] = df['sintoma'].fillna('Sin Especificar')
    df['sistema_afectado'] = df['sistema_afectado'].fillna('Sin Especificar')
    
    print(f"Total registros: {len(df)}")
    print(f"Motivos únicos: {df['motivo'].nunique()}")
    print(f"Síntomas únicos: {df['sintoma'].nunique()}")
    print(f"Sistemas únicos: {df['sistema_afectado'].nunique()}")
    
    print(f"\n✓ Procesado: {len(df)} filas")
    
    return df