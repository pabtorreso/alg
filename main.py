import sys
import os
from services.db import DatabaseService
from pipeline.dm_analisis_compras import procesar_compras
from pipeline.dm_analisis_reprogramacion import procesar_reprogramacion

def main():
    print("="*70)
    print("PIPELINE DE ANÁLISIS DE DATOS")
    print("="*70)
    
    # Crear directorio output si no existe
    os.makedirs('output', exist_ok=True)
    
    db = None
    try:
        # Inicializar servicio de base de datos
        print("\n[1/5] Inicializando conexión a base de datos...")
        db = DatabaseService()
        
        # Ejecutar query de compras
        print("\n[2/5] Extrayendo datos de compras...")
        df_compras = db.execute_query_from_file('querys/qy_analisis_compras.sql')
        
        # Ejecutar query de reprogramación
        print("\n[3/5] Extrayendo datos de reprogramación...")
        df_repro = db.execute_query_from_file('querys/qy_analisis_reprogramacion.sql')
        
        # Procesar compras
        print("\n[4/5] Procesando compras...")
        df_compras_procesado = procesar_compras(df_compras)
        df_compras_procesado.to_csv('output/dm_analisis_compras.csv', index=False)
        print("✓ Guardado: output/dm_analisis_compras.csv")
        
        # Procesar reprogramación
        print("\n[5/5] Procesando reprogramación...")
        df_repro_procesado = procesar_reprogramacion(df_repro)
        df_repro_procesado.to_csv('output/dm_analisis_reprogramacion.csv', index=False)
        print("✓ Guardado: output/dm_analisis_reprogramacion.csv")
        
        print("\n" + "="*70)
        print("✓ PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*70)
        
    except FileNotFoundError as e:
        print(f"\n✗ Error: Archivo no encontrado - {e}")
        print("Verifica que existan los archivos .sql en la carpeta querys/")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n✗ Error durante la ejecución: {e}")
        sys.exit(1)
        
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    main()