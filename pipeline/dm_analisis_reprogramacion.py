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
        
        tipo = filas_equipo['equipo_tipo'].replace(['NULL', '', np.nan], np.nan).mode()
        marca = filas_equipo['equipo_marca'].replace(['NULL', '', np.nan], np.nan).mode()
        modelo = filas_equipo['equipo_modelo'].replace(['NULL', '', np.nan], np.nan).mode()
        
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

def estandarizar_instrucciones(texto):
    if pd.isna(texto) or texto == 'NULL' or texto.strip() == '':
        return 'Sin Especificar'
    
    texto_lower = texto.lower()
    
    if re.search(r'no ejecut', texto_lower):
        return 'No Ejecutada'
    
    if re.search(r'pendiente', texto_lower):
        return 'Pendiente'
    
    if re.search(r'realiz.*según pauta|según pauta', texto_lower):
        return 'Realizar Según Pauta'
    
    if re.search(r'toma.*muestra|muestra.*aceite', texto_lower):
        return 'Toma de Muestra'
    
    if re.search(r'torque|reapriete', texto_lower):
        return 'Ajuste de Torque'
    
    if re.search(r'revisión|revision|inspeccion|inspección', texto_lower):
        return 'Inspección'
    
    if re.search(r'falta.*insumo|sin.*insumo|solicita.*filtro', texto_lower):
        return 'Falta de Insumos'
    
    if re.search(r'proveedor.*certificación|certificación', texto_lower):
        return 'Requiere Certificación'
    
    if re.search(r'realiz', texto_lower):
        return 'Ejecutada'
    
    if re.search(r'programa.*semana', texto_lower):
        return 'Programación Semanal'
    
    if re.search(r'programa.*día|planifica.*día', texto_lower):
        return 'Programación Diaria'
    
    if re.search(r'entrega|salida', texto_lower):
        return 'Entrega de Equipo'
    
    return 'Otra Instrucción'

def estandarizar_motivo_repro_texto(texto):
    """Estandariza solo el texto del motivo"""
    texto_lower = texto.lower()
    
    if re.search(r'error.*planificar|error.*registro', texto_lower):
        return 'Error de Planificación'
    
    if re.search(r'sin.*insumo', texto_lower):
        return 'Falta de Insumos'
    
    if re.search(r'sin.*disponibilidad.*logística|sin transporte', texto_lower):
        return 'Problema Logístico'
    
    if re.search(r'sin.*disponibilidad.*hh|sin.*mecánica', texto_lower):
        return 'Falta de Personal'
    
    if re.search(r'equipo.*inoperativo', texto_lower):
        return 'Equipo Inoperativo'
    
    if re.search(r'incumplimiento.*proveedor', texto_lower):
        return 'Incumplimiento Proveedor'
    
    if re.search(r'operación.*no.*entrega', texto_lower):
        return 'Operación No Entrega'
    
    if re.search(r'suspensión.*decisión.*técnica', texto_lower):
        return 'Decisión Técnica'
    
    if re.search(r'bloqueo.*seguridad', texto_lower):
        return 'Bloqueo Seguridad'
    
    if re.search(r'variación.*utilización', texto_lower):
        return 'Variación Utilización'
    
    if re.search(r'sin.*reporte.*faena', texto_lower):
        return 'Sin Reporte Faena'
    
    if re.search(r'condición.*climática', texto_lower):
        return 'Condición Climática'
    
    return 'Otro Motivo'

def inferir_motivo_repro(row):
    """Infiere el motivo cuando hay reprogramaciones pero sin motivo especificado"""
    
    if pd.notna(row['reprogramaciones_motivo']) and row['reprogramaciones_motivo'].strip() != '':
        return estandarizar_motivo_repro_texto(row['reprogramaciones_motivo'])
    
    cantidad_repro = pd.to_numeric(row['reprogramaciones_cantidad'], errors='coerce')
    if pd.isna(cantidad_repro) or cantidad_repro == 0:
        return 'Sin Reprogramación'
    
    instr = str(row['otm_instrucciones_especiales']).lower()
    disponibilidad = str(row['otm_disponibilidad_insumos']).lower()
    estado = str(row['actividad_estado']).lower()
    
    if disponibilidad == 'no':
        return 'Falta de Insumos'
    
    if 'no ejecut' in instr or 'pendiente' in instr:
        return 'No Ejecutada'
    
    if 'falta' in instr or 'sin insumo' in instr:
        return 'Falta de Insumos'
    
    if 'suspendid' in estado or 'cancelad' in estado:
        return 'Suspendida'
    
    return 'Motivo No Especificado'

def procesar_reprogramacion(df):
    """Procesa el DataFrame de reprogramaciones"""
    print("\n=== PROCESANDO ANÁLISIS DE REPROGRAMACIÓN ===\n")
    
    # Llenar NULLs de equipos
    nulls_antes = (df['equipo_tipo'] == 'NULL').sum() + df['equipo_tipo'].isna().sum()
    df = llenar_datos_equipo(df)
    nulls_desp = (df['equipo_tipo'] == 'NULL').sum() + df['equipo_tipo'].isna().sum()
    print(f"NULLs de equipo llenados: {nulls_antes - nulls_desp}")
    
    # Contar inferencias
    repro_sin_motivo = df[
        (pd.to_numeric(df['reprogramaciones_cantidad'], errors='coerce') > 0) &
        ((df['reprogramaciones_motivo'].isna()) | 
         (df['reprogramaciones_motivo'].str.strip() == ''))
    ].shape[0]
    
    # Estandarizar
    df['otm_instrucciones_especiales'] = df['otm_instrucciones_especiales'].apply(estandarizar_instrucciones)
    df['reprogramaciones_motivo'] = df.apply(inferir_motivo_repro, axis=1)
    
    # Resumen
    print(f"\nMotivos de reprogramación inferidos: {repro_sin_motivo}")
    print("\nInstrucciones Especiales:")
    print(df['otm_instrucciones_especiales'].value_counts())
    print("\n" + "="*50)
    print("\nMotivo de Reprogramación:")
    print(df['reprogramaciones_motivo'].value_counts())
    print(f"\n✓ Procesado: {len(df)} filas")
    
    return df