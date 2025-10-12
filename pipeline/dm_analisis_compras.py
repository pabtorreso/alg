import pandas as pd
import numpy as np
import re

def llenar_datos_equipo(df):
    """Llena los datos de equipo (tipo, marca, modelo) usando equipo_codigo como referencia"""
    
    equipos_dict = {}
    
    for codigo in df['equipo_codigo'].unique():
        if pd.isna(codigo) or codigo == 'NULL':
            continue
            
        filas_equipo = df[df['equipo_codigo'] == codigo]
        
        tipo = filas_equipo['equipo_tipo'].replace(['NULL', '', np.nan], np.nan).infer_objects(copy=False).mode()
        marca = filas_equipo['equipo_marca'].replace(['NULL', '', np.nan], np.nan).infer_objects(copy=False).mode()
        modelo = filas_equipo['equipo_modelo'].replace(['NULL', '', np.nan], np.nan).infer_objects(copy=False).mode()
        
        equipos_dict[codigo] = {
            'tipo': tipo.iloc[0] if len(tipo) > 0 else None,
            'marca': marca.iloc[0] if len(marca) > 0 else None,
            'modelo': modelo.iloc[0] if len(modelo) > 0 else None
        }
    
    def llenar_fila(row):
        codigo = row['equipo_codigo']
        if codigo in equipos_dict:
            datos = equipos_dict[codigo]
            
            if pd.isna(row['equipo_tipo']) or row['equipo_tipo'] == 'NULL' or row['equipo_tipo'] == '':
                if datos['tipo']:
                    row['equipo_tipo'] = datos['tipo']
            
            if pd.isna(row['equipo_marca']) or row['equipo_marca'] == 'NULL' or row['equipo_marca'] == '':
                if datos['marca']:
                    row['equipo_marca'] = datos['marca']
            
            if pd.isna(row['equipo_modelo']) or row['equipo_modelo'] == 'NULL' or row['equipo_modelo'] == '':
                if datos['modelo']:
                    row['equipo_modelo'] = datos['modelo']
        
        return row
    
    return df.apply(llenar_fila, axis=1)

def estandarizar_motivo_compra(texto):
    if pd.isna(texto) or texto == 'NULL':
        return 'Sin Especificar'
    
    texto_lower = texto.lower()
    
    if re.search(r'kit', texto_lower):
        if re.search(r'\d+\s*(hrs?|horas?)', texto_lower):
            return 'Kit Mantenimiento Programado'
        return 'Kit de Mantenimiento'
    
    if re.search(r'preventiv', texto_lower):
        return 'Mantenimiento Preventivo'
    
    if re.search(r'mantencion|mantención|manto', texto_lower):
        return 'Mantenimiento'
    
    if re.search(r'filtro', texto_lower):
        return 'Compra de Filtros'
    
    if re.search(r'servicio', texto_lower):
        return 'Servicio de Mantenimiento'
    
    if re.search(r'reparacion|reparación', texto_lower):
        return 'Reparación'
    
    if re.search(r'cambio', texto_lower):
        return 'Cambio de Componente'
    
    if re.search(r'insumo|reposicion|reposición', texto_lower):
        return 'Reposición de Insumos'
    
    if re.search(r'viaje|traslado', texto_lower):
        return 'Servicio de Traslado'
    
    if re.search(r'compra', texto_lower):
        return 'Compra General'
    
    return 'Otro'

def estandarizar_item_compra(texto):
    if pd.isna(texto) or texto == 'NULL':
        return 'Sin Especificar'
    
    texto_lower = texto.lower()
    
    if re.search(r'filtro', texto_lower):
        if re.search(r'combustible|petroleo|petróleo|diesel', texto_lower):
            return 'Filtro Combustible'
        if re.search(r'aire', texto_lower):
            return 'Filtro Aire'
        if re.search(r'aceite.*motor|motor.*aceite', texto_lower):
            return 'Filtro Aceite Motor'
        if re.search(r'transmision|transmisión', texto_lower):
            return 'Filtro Transmisión'
        if re.search(r'hidraulic', texto_lower):
            return 'Filtro Hidráulico'
        if re.search(r'aceite', texto_lower):
            return 'Filtro Aceite'
        return 'Filtro Otro'
    
    if re.search(r'kit.*servicio|servicio.*kit', texto_lower):
        return 'Kit de Servicio'
    
    if re.search(r'lubricante|mobil|nuto|aceite|grasa|delvac', texto_lower):
        return 'Lubricante'
    
    if re.search(r'servicio|ensayo|horas hombre|traslado', texto_lower):
        return 'Servicio'
    
    if re.search(r'correa', texto_lower):
        return 'Correa'
    
    if re.search(r'junta', texto_lower):
        return 'Junta'
    
    if re.search(r'sello|empaquetadura', texto_lower):
        return 'Sello'
    
    if re.search(r'^\d{5,}', texto):
        return 'Repuesto'
    
    return 'Otro'

def procesar_compras(df):
    """Procesa el DataFrame de compras"""
    print("\n=== PROCESANDO ANÁLISIS DE COMPRAS ===\n")
    
    # Llenar NULLs de equipos
    nulls_antes = (df['equipo_tipo'] == 'NULL').sum() + df['equipo_tipo'].isna().sum()
    df = llenar_datos_equipo(df)
    nulls_desp = (df['equipo_tipo'] == 'NULL').sum() + df['equipo_tipo'].isna().sum()
    print(f"NULLs de equipo llenados: {nulls_antes - nulls_desp}")
    
    # Estandarizar
    df['motivo_compra'] = df['motivo_compra'].apply(estandarizar_motivo_compra)
    df['compra_item'] = df['compra_item'].apply(estandarizar_item_compra)
    
    # Resumen
    print("\nMotivo de Compra:")
    print(df['motivo_compra'].value_counts())
    print("\n" + "="*50)
    print("\nItem de Compra:")
    print(df['compra_item'].value_counts())
    print(f"\n✓ Procesado: {len(df)} filas")
    
    return df