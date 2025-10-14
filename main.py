import sys
import os
from services.db import DatabaseService
from services.db_populate import DatabasePopulateService
from pipeline.dm_analisis_compras import procesar_compras
from pipeline.dm_analisis_reprogramacion import procesar_reprogramacion

def main():
    print("="*70)
    print("PIPELINE DE EXTRACCION Y POBLACION DE CATALOGOS")
    print("="*70)
    
    os.makedirs('output', exist_ok=True)
    
    db_fetch = None
    db_populate = None
    
    try:
        # ===== FASE 1: EXTRACCION =====
        print("\n[FASE 1] EXTRACCION DE DATOS")
        print("="*70)
        
        db_fetch = DatabaseService()
        
        print("\n[1/2] Extrayendo datos de compras...")
        df_compras = db_fetch.execute_query_from_file('queries/fetch/qy_analisis_compras.sql')
        
        print("\n[2/2] Extrayendo datos de reprogramacion...")
        df_repro = db_fetch.execute_query_from_file('queries/fetch/qy_analisis_reprogramacion.sql')
        
        # ===== FASE 2: PROCESAMIENTO =====
        print("\n[FASE 2] PROCESAMIENTO DE DATOS")
        print("="*70)
        
        print("\n[1/2] Procesando compras...")
        df_compras_proc = procesar_compras(df_compras)
        df_compras_proc.to_csv('output/dm_analisis_compras.csv', index=False)
        print("OK - Guardado: output/dm_analisis_compras.csv")
        
        print("\n[2/2] Procesando reprogramacion...")
        df_repro_proc = procesar_reprogramacion(df_repro)
        df_repro_proc.to_csv('output/dm_analisis_reprogramacion.csv', index=False)
        print("OK - Guardado: output/dm_analisis_reprogramacion.csv")
        
        # ===== FASE 3: POBLACION =====
        print("\n[FASE 3] POBLACION DE CATALOGOS")
        print("="*70)
        
        db_populate = DatabasePopulateService()
        
        print("\n[1/2] Creando tablas temporales...")
        df_compras_proc.to_sql('temp_compras', db_populate.engine, if_exists='replace', index=False)
        df_repro_proc.to_sql('temp_reprogramacion', db_populate.engine, if_exists='replace', index=False)
        print("OK - Tablas temporales creadas")
        
        print("\n[2/2] Poblando catalogos...")
        db_populate.execute_query_from_file('queries/populate/populate_catalogos.sql')
        
        print("\n" + "="*70)
        print("OK - PROCESO COMPLETADO")
        print("="*70)
        print("\nCSVs generados en: output/")
        print("Catalogos poblados: MOTIVO_COMPRA, ITEM, MOTIVO_REPROGRAMACION")
        
    except FileNotFoundError as e:
        print(f"\nERROR: Archivo no encontrado - {e}")
        sys.exit(1)
        
    except Exception as e:
        print(f"\nERROR durante la ejecucion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if db_fetch:
            db_fetch.close()
        if db_populate:
            db_populate.close()

if __name__ == "__main__":
    main()