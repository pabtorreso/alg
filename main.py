"""
Pipeline ETL para limpieza y carga de datos
Ejecuta los servicios de reprogramación y compras en orden
"""

import sys
from datetime import datetime
import servicio_limpieza_reprogramacion as pipeline_reprog
import servicio_limpieza_compras as pipeline_compras


def main():
    inicio = datetime.now()
    print("\n" + "="*60)
    print("🚀 INICIANDO PIPELINES ETL")
    print("="*60)
    print(f"⏰ Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    exitoso = True
    
    # Pipeline 1: Reprogramaciones
    #try:
    #    pipeline_reprog.ejecutar_proceso()
    #except Exception as e:
    #    print(f"❌ Error en pipeline de reprogramaciones: {e}")
    #    exitoso = False
    
    # Pipeline 2: Compras
    try:
        pipeline_compras.ejecutar_proceso()
    except Exception as e:
        print(f"❌ Error en pipeline de compras: {e}")
        exitoso = False
    
    # Resumen final
    fin = datetime.now()
    duracion = (fin - inicio).total_seconds()
    
    print("="*60)
    if exitoso:
        print("✅ TODOS LOS PIPELINES COMPLETADOS EXITOSAMENTE")
    else:
        print("⚠️  PIPELINES COMPLETADOS CON ERRORES")
    print(f"⏰ Duración total: {duracion:.2f} segundos")
    print("="*60 + "\n")
    
    return 0 if exitoso else 1


if __name__ == "__main__":
    sys.exit(main())