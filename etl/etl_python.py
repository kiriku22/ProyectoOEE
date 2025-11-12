import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime
import re
import tkinter as tk
from tkinter import filedialog, messagebox

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExcelToMySQLEtl:
    def __init__(self, excel_file_path=None, db_connection_string=None):
        """
        Inicializa el ETL
        
        Args:
            excel_file_path (str): Ruta del archivo Excel (opcional)
            db_connection_string (str): String de conexión a MySQL
        """
        self.excel_file_path = excel_file_path
        self.db_connection_string = db_connection_string
        self.engine = None
        self.dataframes = {}
        
    def select_excel_file(self):
        """Abre un diálogo para seleccionar el archivo Excel"""
        try:
            root = tk.Tk()
            root.withdraw()  # Ocultar la ventana principal
            
            file_path = filedialog.askopenfilename(
                title="Selecciona el archivo Excel SEGUIMIENTO TEMPERAS Y VINILOS",
                filetypes=[
                    ("Excel files", "*.xlsx *.xls"),
                    ("All files", "*.*")
                ]
            )
            
            if file_path:
                self.excel_file_path = file_path
                logger.info(f"Archivo seleccionado: {self.excel_file_path}")
                return True
            else:
                logger.error("No se seleccionó ningún archivo")
                return False
                
        except Exception as e:
            logger.error(f"Error al seleccionar archivo: {e}")
            return False
    
    def connect_to_mysql(self):
        """Establece conexión con MySQL"""
        try:
            self.engine = create_engine(self.db_connection_string)
            
            # Verificar la conexión
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info("Conexión a MySQL establecida exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error al conectar con MySQL: {e}")
            return False
    
    def read_excel_file(self):
        """
        Lee el archivo Excel y carga todas las tablas de la hoja 'base de datos'
        """
        try:
            if not self.excel_file_path or not os.path.exists(self.excel_file_path):
                logger.error(f"Archivo no encontrado: {self.excel_file_path}")
                return False
            
            logger.info(f"Leyendo archivo Excel: {self.excel_file_path}")
            
            # Leer el archivo Excel
            excel_file = pd.ExcelFile(self.excel_file_path)
            
            # Verificar que existe la hoja 'base de datos'
            if 'base de datos' not in excel_file.sheet_names:
                logger.error("No se encontró la hoja 'base de datos' en el archivo Excel")
                logger.info(f"Hojas disponibles: {excel_file.sheet_names}")
                return False
            
            # Leer la hoja 'base de datos'
            df = pd.read_excel(self.excel_file_path, sheet_name='base de datos')
            logger.info(f"Hoja 'base de datos' cargada. Dimensiones: {df.shape}")
            
            # Identificar tablas separadas
            self.identify_tables(df)
            
            return True
            
        except Exception as e:
            logger.error(f"Error al leer el archivo Excel: {e}")
            return False
    
    def identify_tables(self, df):
        """
        Identifica y separa las diferentes tablas dentro de la hoja 'base de datos'
        """
        try:
            # Limpiar DataFrames previos
            self.dataframes = {}
            
            # Estrategia 1: Buscar filas completamente vacías como separadores
            empty_rows = df.isnull().all(axis=1)
            empty_row_indices = empty_rows[empty_rows].index.tolist()
            
            # Estrategia 2: Buscar filas con texto en la primera columna y vacías en las demás
            potential_headers = []
            for idx, row in df.iterrows():
                if pd.notna(row.iloc[0]) and (pd.isna(row.iloc[1:]).all() or row.iloc[1:].eq(0).all()):
                    potential_headers.append(idx)
            
            # Combinar todos los puntos de separación
            split_points = sorted(set(empty_row_indices + potential_headers))
            
            # Dividir el DataFrame en tablas
            start_idx = 0
            table_count = 0
            
            for split_point in split_points:
                if split_point > start_idx + 1:  # Al menos 2 filas para ser una tabla válida
                    table_df = df.iloc[start_idx:split_point].copy()
                    # Verificar que la tabla tiene datos válidos
                    if not table_df.empty and len(table_df.columns) > 0:
                        table_name = f"tabla_{table_count}"
                        self.dataframes[table_name] = table_df
                        logger.info(f"Tabla '{table_name}' identificada con {len(table_df)} filas y {len(table_df.columns)} columnas")
                        table_count += 1
                start_idx = split_point + 1
            
            # Agregar la última tabla si tiene datos
            if start_idx < len(df) - 1:  # Al menos 2 filas
                table_df = df.iloc[start_idx:].copy()
                if not table_df.empty and len(table_df.columns) > 0:
                    table_name = f"tabla_{table_count}"
                    self.dataframes[table_name] = table_df
                    logger.info(f"Tabla '{table_name}' identificada con {len(table_df)} filas y {len(table_df.columns)} columnas")
            
            # Si no se encontraron separadores, usar todo el DataFrame como una tabla
            if not self.dataframes:
                self.dataframes["tabla_principal"] = df
                logger.info("Usando todo el DataFrame como tabla principal")
                
            logger.info(f"Total de tablas identificadas: {len(self.dataframes)}")
                
        except Exception as e:
            logger.error(f"Error al identificar tablas: {e}")
    
    def clean_data(self):
        """
        Realiza la limpieza de datos para todas las tablas
        """
        try:
            logger.info("Iniciando limpieza de datos...")
            
            cleaned_dataframes = {}
            
            for table_name, df in self.dataframes.items():
                logger.info(f"Limpiando tabla: {table_name}")
                
                # 1. Eliminar filas completamente vacías
                df_clean = df.dropna(how='all')
                
                if df_clean.empty:
                    logger.warning(f"Tabla {table_name} vacía después de eliminar filas vacías")
                    continue
                
                # 2. Eliminar columnas completamente vacías
                df_clean = df_clean.dropna(axis=1, how='all')
                
                if df_clean.empty:
                    logger.warning(f"Tabla {table_name} vacía después de eliminar columnas vacías")
                    continue
                
                # 3. Reemplazar valores nulos con 0
                df_clean = df_clean.fillna(0)
                
                # 4. Limpiar nombres de columnas
                df_clean.columns = [self.clean_column_name(col) for col in df_clean.columns]
                
                # 5. Convertir tipos de datos apropiados
                df_clean = self.convert_data_types(df_clean)
                
                # 6. Resetear índice
                df_clean = df_clean.reset_index(drop=True)
                
                # 7. Eliminar filas donde todas las columnas son 0 (excepto posiblemente la primera)
                non_zero_rows = (df_clean.iloc[:, 1:] != 0).any(axis=1) | (df_clean.iloc[:, 0].notna())
                df_clean = df_clean[non_zero_rows]
                
                # Actualizar el DataFrame limpio
                cleaned_dataframes[table_name] = df_clean
                
                logger.info(f"Tabla {table_name} limpiada. Nuevas dimensiones: {df_clean.shape}")
            
            self.dataframes = cleaned_dataframes
            logger.info(f"Limpieza completada. Tablas restantes: {len(self.dataframes)}")
                
        except Exception as e:
            logger.error(f"Error en la limpieza de datos: {e}")
    
    def clean_column_name(self, column_name):
        """
        Limpia el nombre de la columna para hacerlo compatible con SQL
        """
        if pd.isna(column_name):
            return "columna_desconocida"
        
        # Convertir a string
        col_name = str(column_name).strip().lower()
        
        # Reemplazar caracteres especiales y espacios
        col_name = re.sub(r'[^\w]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name)
        col_name = col_name.strip('_')
        
        # Si el nombre está vacío después de la limpieza
        if not col_name:
            return "columna_desconocida"
            
        return col_name
    
    def convert_data_types(self, df):
        """
        Convierte los tipos de datos de las columnas
        """
        for col in df.columns:
            # Intentar convertir a numérico donde sea posible
            if df[col].dtype == 'object':
                # Intentar convertir a numérico
                numeric_vals = pd.to_numeric(df[col], errors='coerce')
                # Si más del 80% de los valores se pueden convertir, usar numérico
                if numeric_vals.notna().sum() / len(df) > 0.8:
                    df[col] = numeric_vals.fillna(0)
            
            # Convertir booleanos a enteros
            if df[col].dtype == 'bool':
                df[col] = df[col].astype(int)
                
        return df
    
    def load_to_mysql(self):
        """
        Carga todas las tablas limpias a MySQL
        """
        try:
            if not self.engine:
                logger.error("No hay conexión a MySQL establecida")
                return False
            
            if not self.dataframes:
                logger.error("No hay datos para cargar")
                return False
            
            logger.info("Iniciando carga de datos a MySQL...")
            
            for table_name, df in self.dataframes.items():
                if df.empty:
                    logger.warning(f"La tabla {table_name} está vacía, omitiendo...")
                    continue
                
                # Crear nombre de tabla seguro para MySQL
                safe_table_name = f"seguimiento_temperas_{table_name}"
                safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', safe_table_name)
                
                try:
                    # Cargar DataFrame a MySQL
                    df.to_sql(
                        name=safe_table_name,
                        con=self.engine,
                        if_exists='replace',
                        index=False,
                        index_label='id',
                        chunksize=1000
                    )
                    logger.info(f"Tabla {safe_table_name} cargada exitosamente a MySQL ({len(df)} registros)")
                    
                except Exception as e:
                    logger.error(f"Error al cargar tabla {safe_table_name}: {e}")
            
            logger.info("Carga a MySQL completada")
            return True
            
        except Exception as e:
            logger.error(f"Error en la carga a MySQL: {e}")
            return False
    
    def run_etl(self):
        """
        Ejecuta el proceso completo ETL
        """
        logger.info("Iniciando proceso ETL...")
        
        # Paso 1: Seleccionar archivo Excel si no se proporcionó
        if not self.excel_file_path:
            if not self.select_excel_file():
                return False
        
        # Paso 2: Conectar a MySQL
        if not self.connect_to_mysql():
            return False
        
        # Paso 3: Leer archivo Excel
        if not self.read_excel_file():
            return False
        
        # Paso 4: Limpiar datos
        self.clean_data()
        
        # Paso 5: Cargar a MySQL
        if not self.load_to_mysql():
            return False
        
        logger.info("Proceso ETL completado exitosamente")
        return True
    
    def get_table_info(self):
        """
        Retorna información sobre las tablas procesadas
        """
        info = {}
        for table_name, df in self.dataframes.items():
            info[table_name] = {
                'filas': len(df),
                'columnas': len(df.columns),
                'columnas_nombres': list(df.columns)
            }
        return info

# Función principal para ejecutar el ETL
def main():
    # Configuración de conexión a MySQL (ajusta según tu configuración)
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'tu_usuario',
        'password': 'tu_contraseña',
        'database': 'seguimiento_temperas'
    }
    
    # Crear string de conexión
    connection_string = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    
    # Crear y ejecutar ETL
    etl = ExcelToMySQLEtl(db_connection_string=connection_string)
    
    # Ejecutar proceso ETL
    success = etl.run_etl()
    
    # Mostrar información de las tablas procesadas
    if success:
        table_info = etl.get_table_info()
        print("\n" + "="*60)
        print("RESUMEN DEL PROCESO ETL")
        print("="*60)
        for table_name, info in table_info.items():
            print(f"📊 {table_name}: {info['filas']} filas, {info['columnas']} columnas")
        print("="*60)
    else:
        print("❌ El proceso ETL falló. Revisa los logs para más información.")

if __name__ == "__main__":
    main()