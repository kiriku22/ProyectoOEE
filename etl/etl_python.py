import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import logging

class OEEETL:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.connect()
    
    def connect(self):
        """Conectar a PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logging.info("Conexión a BD establecida")
        except Exception as e:
            logging.error(f"Error conectando a BD: {e}")
    
    def extract_from_excel(self, file_path):
        """Extraer datos de Excel"""
        try:
            # Leer datos de producción
            df_produccion = pd.read_excel(file_path, sheet_name='Produccion')
            df_paros = pd.read_excel(file_path, sheet_name='Paros')
            
            logging.info(f"Datos extraídos: {len(df_produccion)} registros producción, {len(df_paros)} paros")
            return df_produccion, df_paros
            
        except Exception as e:
            logging.error(f"Error extrayendo datos Excel: {e}")
            return None, None
    
    def transform_production_data(self, df):
        """Transformar datos de producción"""
        # Limpieza y transformación
        df_clean = df.copy()
        
        # Convertir fechas
        df_clean['fecha'] = pd.to_datetime(df_clean['fecha']).dt.date
        df_clean['hora_inicio'] = pd.to_datetime(df_clean['hora_inicio']).dt.time
        df_clean['hora_fin'] = pd.to_datetime(df_clean['hora_fin']).dt.time
        
        # Validar cantidades
        df_clean = df_clean[df_clean['cantidad_producida'] >= 0]
        df_clean = df_clean[df_clean['cantidad_defectuosa'] <= df_clean['cantidad_producida']]
        
        return df_clean
    
    def load_production_data(self, df):
        """Cargar datos de producción a BD"""
        cursor = self.conn.cursor()
        
        insert_query = """
        INSERT INTO fact_registro_produccion 
        (maquina_id, turno_id, producto_id, fecha, hora_inicio, hora_fin, cantidad_producida, cantidad_defectuosa)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING registro_id
        """
        
        try:
            for _, row in df.iterrows():
                cursor.execute(insert_query, (
                    row['maquina_id'], row['turno_id'], row['producto_id'],
                    row['fecha'], row['hora_inicio'], row['hora_fin'],
                    row['cantidad_producida'], row['cantidad_defectuosa']
                ))
            
            self.conn.commit()
            logging.info("Datos de producción cargados exitosamente")
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error cargando datos producción: {e}")
    
    def run_etl(self, excel_file_path):
        """Ejecutar proceso ETL completo"""
        logging.info("Iniciando proceso ETL")
        
        # Extraer
        df_prod, df_paros = self.extract_from_excel(excel_file_path)
        
        if df_prod is not None:
            # Transformar
            df_prod_transformed = self.transform_production_data(df_prod)
            
            # Cargar
            self.load_production_data(df_prod_transformed)
        
        logging.info("Proceso ETL completado")

# Configuración y ejecución
if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'database': 'oee_database',
        'user': 'postgres',
        'password': 'tu_password'
    }
    
    etl = OEEETL(db_config)
    etl.run_etl('datos_produccion.xlsx')