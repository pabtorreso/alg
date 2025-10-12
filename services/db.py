import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseService:
    def __init__(self):
        self.engine = None
        self._initialize_engine()
    
    def _initialize_engine(self):
        """Inicializa el engine de SQLAlchemy"""
        try:
            connection_string = (
                f"postgresql://{os.getenv('ERP_DB_USER')}:{os.getenv('ERP_DB_PASSWORD')}"
                f"@{os.getenv('ERP_DB_HOST')}:{os.getenv('ERP_DB_PORT')}/{os.getenv('ERP_DB_NAME')}"
            )
            self.engine = create_engine(connection_string)
            print("✓ Engine de base de datos inicializado correctamente")
        except Exception as e:
            print(f"✗ Error al inicializar engine: {e}")
            raise
    
    def execute_query(self, query):
        """Ejecuta una query y retorna un DataFrame"""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(text(query), conn)
                print(f"✓ Query ejecutada: {len(df)} filas obtenidas")
                return df
        except Exception as e:
            print(f"✗ Error al ejecutar query: {e}")
            raise
    
    def execute_query_from_file(self, filepath):
        """Lee y ejecuta una query desde un archivo .sql"""
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                query = file.read()
            print(f"✓ Query leída desde: {filepath}")
            return self.execute_query(query)
        except Exception as e:
            print(f"✗ Error al leer/ejecutar archivo {filepath}: {e}")
            raise
    
    def close(self):
        """Cierra el engine"""
        if self.engine:
            self.engine.dispose()
            print("✓ Engine de base de datos cerrado")