import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

class DatabasePopulateService:
    def __init__(self):
        self.engine = None
        self._initialize_engine()
    
    def _initialize_engine(self):
        """Inicializa el engine para la BD de población"""
        try:
            connection_string = (
                f"postgresql://{os.getenv('POPULATE_DB_USER')}:{os.getenv('POPULATE_DB_PASSWORD')}"
                f"@{os.getenv('POPULATE_DB_HOST')}:{os.getenv('POPULATE_DB_PORT')}/{os.getenv('POPULATE_DB_NAME')}"
            )
            self.engine = create_engine(connection_string)
            print("✓ Engine de BD de población inicializado")
        except Exception as e:
            print(f"✗ Error al inicializar engine de población: {e}")
            raise
    
    def execute_populate_query(self, query, params=None):
        """Ejecuta una query de población (INSERT/UPDATE)"""
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    if params:
                        conn.execute(text(query), params)
                    else:
                        conn.execute(text(query))
                    trans.commit()
                    print(f"✓ Query de población ejecutada")
                except Exception as e:
                    trans.rollback()
                    raise e
        except Exception as e:
            print(f"✗ Error al ejecutar query de población: {e}")
            raise
    
    def populate_from_dataframe(self, df, table_name, if_exists='append'):
        """Inserta un DataFrame completo en una tabla"""
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            print(f"✓ {len(df)} filas insertadas en {table_name}")
        except Exception as e:
            print(f"✗ Error al insertar en {table_name}: {e}")
            raise
    
    def execute_query_from_file(self, filepath, params=None):
        """Lee y ejecuta una query desde archivo"""
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                query = file.read()
            print(f"✓ Query leída desde: {filepath}")
            self.execute_populate_query(query, params)
        except Exception as e:
            print(f"✗ Error al ejecutar {filepath}: {e}")
            raise
    
    def close(self):
        """Cierra el engine"""
        if self.engine:
            self.engine.dispose()
            print("✓ Engine de población cerrado")