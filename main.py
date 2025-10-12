import sys
import os
from services.db import DatabaseService
# from services.db_populate import DatabasePopulateService
from pipeline.dm_analisis_compras import procesar_compras
from pipeline.dm_analisis_reprogramacion import procesar_reprogramacion
from pipeline.dm_analisis_prox_m import procesar_proximo_mantenimiento
from pipeline.dm_analisis_tiempo_fuera import procesar_tiempo_fuera
from pipeline.dm_analisis_notificaciones import procesar_notificaciones

def main():
    print("="*70)
    print("PIPELINE DE EXTRACCIÓN Y PROCESAMIENTO DE DATOS")
    print("="*70)
    
    # Crear directorio output si no existe
    os.makedirs('output', exist_ok=True)
    
    db_fetch = None
    # db_populate = None
    
    try:
        # ===== FASE 1: EXTRACCIÓN =====
        print("\n[FASE 1] EXTRACCIÓN DE DATOS")
        print("="*70)
        
        db_fetch = DatabaseService()
        
        print("\n[1/5] Extrayendo datos de compras...")
        df_compras = db_fetch.execute_query_from_file('queries/fetch/qy_analisis_compras.sql')
        
        print("\n[2/5] Extrayendo datos de reprogramación...")
        df_repro = db_fetch.execute_query_from_file('queries/fetch/qy_analisis_reprogramacion.sql')
        
        print("\n[3/5] Extrayendo datos de próximo mantenimiento...")
        df_prox_m = db_fetch.execute_query_from_file('queries/fetch/qy_analisis_prox_m.sql')
        
        print("\n[4/5] Extrayendo datos de tiempo fuera...")
        df_tiempo_fuera = db_fetch.execute_query_from_file('queries/fetch/qy_analisis_tiempo_fuera.sql')
        
        print("\n[5/5] Extrayendo datos de notificaciones...")
        df_notificaciones = db_fetch.execute_query_from_file('queries/fetch/qy_analisis_notificaciones.sql')
        
        # ===== FASE 2: PROCESAMIENTO =====
        print("\n[FASE 2] PROCESAMIENTO DE DATOS")
        print("="*70)
        
        print("\n[1/5] Procesando compras...")
        df_compras_proc = procesar_compras(df_compras)
        df_compras_proc.to_csv('output/dm_analisis_compras.csv', index=False)
        print("✓ Guardado: output/dm_analisis_compras.csv")
        
        print("\n[2/5] Procesando reprogramación...")
        df_repro_proc = procesar_reprogramacion(df_repro)
        df_repro_proc.to_csv('output/dm_analisis_reprogramacion.csv', index=False)
        print("✓ Guardado: output/dm_analisis_reprogramacion.csv")
        
        print("\n[3/5] Procesando próximo mantenimiento...")
        df_prox_m_proc = procesar_proximo_mantenimiento(df_prox_m)
        df_prox_m_proc.to_csv('output/dm_analisis_prox_m.csv', index=False)
        print("✓ Guardado: output/dm_analisis_prox_m.csv")
        
        print("\n[4/5] Procesando tiempo fuera...")
        df_tiempo_fuera_proc = procesar_tiempo_fuera(df_tiempo_fuera)
        df_tiempo_fuera_proc.to_csv('output/dm_analisis_tiempo_fuera.csv', index=False)
        print("✓ Guardado: output/dm_analisis_tiempo_fuera.csv")
        
        print("\n[5/5] Procesando notificaciones...")
        df_notificaciones_proc = procesar_notificaciones(df_notificaciones)
        df_notificaciones_proc.to_csv('output/dm_analisis_notificaciones.csv', index=False)
        print("✓ Guardado: output/dm_analisis_notificaciones.csv")
        
        # ===== FASE 3: POBLACIÓN (COMENTADA) =====
        # print("\n[FASE 3] POBLACIÓN DE BASE DE DATOS")
        # print("="*70)
        
        # db_populate = DatabasePopulateService()
        
        # print("\n[1/5] Creando tablas temporales...")
        # df_compras_proc.to_sql('temp_compras', db_populate.engine, if_exists='replace', index=False)
        # df_repro_proc.to_sql('temp_reprogramacion', db_populate.engine, if_exists='replace', index=False)
        # df_prox_m_proc.to_sql('temp_prox_mantenimiento', db_populate.engine, if_exists='replace', index=False)
        # df_tiempo_fuera_proc.to_sql('temp_tiempo_fuera', db_populate.engine, if_exists='replace', index=False)
        # df_notificaciones_proc.to_sql('temp_notificaciones', db_populate.engine, if_exists='replace', index=False)
        # print("✓ Tablas temporales creadas")
        
        # print("\n[2/5] Poblando catálogos...")
        # db_populate.execute_query_from_file('queries/populate/01_catalogos.sql')
        
        # print("\n[3/5] Poblando compras...")
        # db_populate.execute_query_from_file('queries/populate/02_compras.sql')
        
        # print("\n[4/5] Poblando reprogramaciones y mantenimiento...")
        # db_populate.execute_query_from_file('queries/populate/03_reprogramacion.sql')
        # db_populate.execute_query_from_file('queries/populate/04_prox_mantenimiento_tiempo_fuera.sql')
        
        # print("\n[5/5] Poblando notificaciones...")
        # db_populate.execute_query_from_file('queries/populate/05_notificaciones.sql')
        
        print("\n" + "="*70)
        print("✓ EXTRACCIÓN Y PROCESAMIENTO COMPLETADO")
        print("="*70)
        print("\nCSVs generados en: output/")
        print("NOTA: Población de BD comentada. Descomenta FASE 3 en main.py para poblar.")
        
    except FileNotFoundError as e:
        print(f"\n✗ Error: Archivo no encontrado - {e}")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n✗ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if db_fetch:
            db_fetch.close()
        # if db_populate:
        #     db_populate.close()

if __name__ == "__main__":
    main()