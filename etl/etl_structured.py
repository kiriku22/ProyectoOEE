# etl_temperas_vinilos_6_tablas_robusto.py
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
import os
import re
import argparse
from pathlib import Path
import getpass

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_process.log')
    ]
)
logger = logging.getLogger(__name__)

class TemperasVinilosETL:
    def __init__(self, excel_file_path=None, db_config=None):
        self.excel_file_path = excel_file_path
        self.db_config = db_config or {}
        self.engine = None
        self.dataframe = None
        
    def get_series_from_dataframe(self, df, col):
        """Obtiene una Series de un DataFrame de forma robusta"""
        try:
            result = df[col]
            
            if isinstance(result, pd.DataFrame):
                if result.shape[1] > 0:
                    return result.iloc[:, 0]
                else:
                    return pd.Series([], dtype=object, name=col)
            elif isinstance(result, pd.Series):
                return result
            else:
                return pd.Series(result, name=col)
                
        except KeyError:
            return pd.Series([], dtype=object, name=col)
        except Exception as e:
            print(f"   ⚠️  Error obteniendo columna '{col}': {e}")
            return pd.Series([], dtype=object, name=col)
    
    def eliminar_columnas_duplicadas(self, df):
        """Elimina columnas duplicadas manteniendo la primera ocurrencia"""
        print(f"\n🔄 Eliminando columnas duplicadas...")
        
        columnas_antes = len(df.columns)
        columnas_duplicadas = df.columns[df.columns.duplicated()].tolist()
        
        if columnas_duplicadas:
            print(f"🚫 Columnas duplicadas encontradas: {set(columnas_duplicadas)}")
        
        df_sin_duplicados = df.loc[:, ~df.columns.duplicated()]
        columnas_despues = len(df_sin_duplicados.columns)
        eliminadas = columnas_antes - columnas_despues
        
        print(f"📊 Columnas antes: {columnas_antes}, después: {columnas_despues}, eliminadas: {eliminadas}")
        
        return df_sin_duplicados

    def mostrar_columnas_disponibles(self, df):
        """Muestra todas las columnas disponibles en el DataFrame"""
        print(f"\n📋 COLUMNAS DISPONIBLES EN EL DATAFRAME:")
        print("="*50)
        for i, col in enumerate(df.columns, 1):
            col_series = self.get_series_from_dataframe(df, col)
            dtype = str(col_series.dtype) if not col_series.empty else 'empty'
            print(f"  {i:2d}. {col} ({dtype})")

    def find_excel_file(self):
        """Busca automáticamente el archivo Excel en el proyecto"""
        try:
            current_dir = Path.cwd()
            
            patterns = [
                "**/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm",
                "**/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsx", 
                "**/SEGUIMIENTO TEMPERAS*.xls*",
                "**/*TEMPERAS*.xls*",
                "**/*VINILOS*.xls*",
                "**/*.xls*"
            ]
            
            for pattern in patterns:
                files = list(current_dir.glob(pattern))
                for file in files:
                    if file.exists():
                        self.excel_file_path = str(file)
                        logger.info(f"📁 Archivo encontrado: {self.excel_file_path}")
                        return True
            
            print("❌ No se encontraron archivos Excel en el proyecto")
            return False
            
        except Exception as e:
            logger.error(f"Error buscando archivo Excel: {e}")
            return False

    def validate_file_path(self):
        """Valida y corrige la ruta del archivo"""
        if not self.excel_file_path:
            return False
            
        file_path = Path(self.excel_file_path)
        
        if not file_path.is_absolute():
            file_path = Path.cwd() / file_path
        
        if file_path.exists():
            self.excel_file_path = str(file_path)
            return True
        
        return self.find_excel_file()

    def connect_to_mysql(self):
        """Establece conexión con MySQL"""
        try:
            connection_string = f"mysql+mysqlconnector://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info("✅ Conectado a MySQL")
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando a MySQL: {e}")
            return False

    def read_excel_with_complex_headers(self):
        """Lee el archivo Excel con estructura compleja de encabezados"""
        try:
            if not self.validate_file_path():
                logger.error("No se pudo encontrar el archivo Excel")
                return False
            
            logger.info(f"📖 Leyendo archivo Excel complejo: {self.excel_file_path}")
            
            excel_file = pd.ExcelFile(self.excel_file_path)
            
            print(f"\n📋 Hojas disponibles en el archivo:")
            for i, sheet_name in enumerate(excel_file.sheet_names, 1):
                print(f"  {i}. {sheet_name}")
            
            # Buscar la hoja 'Base De Datos'
            target_sheet = None
            for sheet_name in excel_file.sheet_names:
                if 'base de datos' in sheet_name.lower():
                    target_sheet = sheet_name
                    break
            
            if not target_sheet:
                target_sheet = excel_file.sheet_names[0]
                print(f"⚠️  Usando hoja: {target_sheet}")
            else:
                print(f"✅ Hoja encontrada: {target_sheet}")
            
            # Leer las primeras filas para analizar la estructura
            df_raw = pd.read_excel(self.excel_file_path, sheet_name=target_sheet, header=None, nrows=10)
            
            print(f"\n🔍 Analizando estructura del archivo...")
            print("Primeras 10 filas crudas:")
            print(df_raw.to_string())
            
            # Encontrar la fila donde empiezan los datos reales
            data_start_row = self.find_data_start_row(df_raw)
            print(f"\n📊 Fila donde inician los datos: {data_start_row}")
            
            # Leer los datos con los encabezados correctos
            df = pd.read_excel(self.excel_file_path, sheet_name=target_sheet, header=data_start_row)
            
            # Limpiar nombres de columnas
            df.columns = self.clean_complex_headers(df.columns)
            
            # Mostrar estructura encontrada
            self.show_detected_structure(df)
            
            self.dataframe = df
            return True
            
        except Exception as e:
            logger.error(f"❌ Error leyendo Excel: {e}")
            return False

    def find_data_start_row(self, df_raw):
        """Encuentra la fila donde empiezan los datos reales"""
        for i in range(len(df_raw)):
            row = df_raw.iloc[i]
            if self.is_data_header_row(row):
                return i
        return 0

    def is_data_header_row(self, row):
        """Determina si una fila es el encabezado de datos"""
        row_str = ' '.join([str(x) for x in row if pd.notna(x)])
        patterns = [
            'fecha', 'mes', 'año', 'maquina', 'operario', 'referencia',
            'unidad', 'display', 'paca', 'horas', 'turno', 'paro'
        ]
        row_lower = row_str.lower()
        matches = sum(1 for pattern in patterns if pattern in row_lower)
        return matches >= 3

    def clean_complex_headers(self, columns):
        """Limpia encabezados complejos y los mapea a nombres estandarizados"""
        cleaned_columns = []
        column_mapping = {}
        
        for i, col in enumerate(columns):
            original_col = str(col) if pd.notna(col) else f"columna_{i}"
            cleaned_col = self.clean_column_name(original_col)
            mapped_col = self.map_specific_columns(cleaned_col, original_col)
            cleaned_columns.append(mapped_col)
            column_mapping[original_col] = mapped_col
        
        print(f"\n🔄 MAPEO DE COLUMNAS DETECTADAS:")
        print("="*50)
        for original, cleaned in column_mapping.items():
            print(f"'{original}' -> '{cleaned}'")
        
        return cleaned_columns

    def map_specific_columns(self, cleaned_col, original_col):
        """Mapea columnas específicas basado en patrones"""
        original_lower = original_col.lower()
        cleaned_lower = cleaned_col.lower()
        
        mapping_patterns = {
            'fecha': 'fecha', 'mes': 'mes', 'año': 'año', 'afio': 'año',
            'maquina': 'maquina', 'operario': 'operario', 'referencia': 'referencia',
            'product': 'producto', 'unidad': 'unidades_producidas_cada_minuto',
            'display': 'unidad_por_display', 'paca': 'display_por_paca',
            'peas': 'pacas_producidas', 'horas': 'horas_trabajadas',
            'turno': 'turno', 'paro': 'tipo_paro', 'observacion': 'observaciones',
            'personal': 'personal_involucrado', 'area': 'area_involucrada_en_subcodigo',
            'subcodigo': 'subcodigo_5', 'codigo_1': 'codigo_de_paro_1',
            'sub_codigo_1': 'sub_codigo_de_paro_1', 'codigo_1_horas': 'codigo_1_en_horas',
            'codigo_2': 'codigo_de_paro_2', 'codigo_2_horas': 'codigo_2_en_horas',
            'codigo_3': 'codigo_de_paro_3', 'subcodigo_3': 'subcodigo_3',
            'codigo_3_horas': 'codigo_3_en_horas', 'codigo_4': 'codigo_de_paro_4',
            'codigo_4_horas': 'codigo_4_en_horas', 'codigo_5': 'codigo_de_paro_5',
            'codigo_5_horas': 'codigo_5_en_horas', 'codigo_6': 'codigo_de_paro_6',
            'codigo_6_horas': 'codigo_6_en_horas', 'codigo_7': 'codigo_de_paro_7',
            'codigo_7_horas': 'codigo_7_en_horas', 'codigo_8': 'codigo_de_paro_8',
            'codigo_8_horas': 'codigo_8_en_horas', 'codigo_9': 'codigo_de_paro_9',
            'codigo_9_horas': 'codigo_9_en_horas', 'codigo_10': 'codigo_de_paro_10',
            'codigo_10_horas': 'codigo_10_en_horas', 'codigo_11': 'codigo_de_paro_11',
            'codigo_11_horas': 'codigo_11_en_horas', 'codigo_12': 'codigo_de_paro_12',
            'codigo_12_horas': 'codigo_12_en_horas', 'codigo_13': 'codigo_de_paro_13',
            'codigo_13_horas': 'codigo_13_en_horas', 'codigo_14': 'codigo_de_paro_14',
            'codigo_14_horas': 'codigo_14_en_horas', 'codigo_15': 'codigo_de_paro_15',
            'codigo_15_horas': 'codigo_15_en_horas', 'codigo_16': 'codigo_de_paro_16',
            'codigo_16_horas': 'codigo_16_en_horas', 'codigo_17': 'codigo_de_paro_17',
            'codigo_17_horas': 'codigo_17_en_horas', 'codigo_18': 'codigo_de_paro_18',
            'codigo_18_horas': 'codigo_18_en_horas', 'tiempo_paro': 'tiempo_de_paro',
            'horas_no_trabajadas': 'horas_no_trabajadas'
        }
        
        for pattern, mapped_name in mapping_patterns.items():
            if pattern in cleaned_lower or pattern in original_lower:
                return mapped_name
        
        return cleaned_col

    def clean_column_name(self, column_name):
        """Limpia nombres de columnas para SQL"""
        if pd.isna(column_name):
            return "columna_desconocida"
        
        col_name = str(column_name).strip().lower()
        col_name = re.sub(r'\s*(pacas|hrs|horas)$', '', col_name)
        col_name = re.sub(r'[^\w]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name)
        col_name = col_name.strip('_')
        
        return col_name or "columna_desconocida"

    def show_detected_structure(self, df):
        """Muestra la estructura detectada del archivo"""
        print(f"\n" + "="*70)
        print("ESTRUCTURA DETECTADA DEL ARCHIVO")
        print("="*70)
        print(f"📊 Dimensiones: {df.shape[0]} filas × {df.shape[1]} columnas")
        print(f"📋 Columnas detectadas:")
        
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
        
        print(f"\n🔍 Muestra de datos (primeras 5 filas):")
        if len(df) > 0:
            print(df.head().to_string())
        else:
            print("No hay datos para mostrar")

    def clean_and_transform_data(self):
        """Limpia y transforma los datos"""
        try:
            logger.info("🧹 Limpiando y transformando datos...")
            
            if self.dataframe is None or self.dataframe.empty:
                logger.error("No hay datos para limpiar")
                return False
            
            df = self.dataframe.copy()
            
            print(f"\n" + "="*70)
            print("LIMPIEZA Y TRANSFORMACIÓN DE DATOS")
            print("="*70)
            
            # 1. Eliminar filas completamente vacías
            initial_rows = len(df)
            df = df.dropna(how='all')
            removed_rows = initial_rows - len(df)
            print(f"📊 Filas eliminadas (vacías): {removed_rows}")
            
            if df.empty:
                print("❌ No hay datos después de la limpieza inicial")
                return False
            
            # 2. Eliminar columnas completamente vacías
            initial_cols = len(df.columns)
            df = df.dropna(axis=1, how='all')
            removed_cols = initial_cols - len(df.columns)
            print(f"📊 Columnas eliminadas (vacías): {removed_cols}")
            
            # 3. Procesar columnas por tipo
            print(f"\n🔄 Procesando columnas por tipo...")
            
            for col in list(df.columns):
                col_series = self.get_series_from_dataframe(df, col)
                
                if col_series.empty:
                    continue
                
                before_nulls = col_series.isna().sum()
                
                # Columnas numéricas conocidas
                numeric_columns = ['mes', 'año', 'pacas_producidas', 'horas_trabajadas', 
                                 'horas_no_trabajadas', 'codigo_1_en_horas', 'codigo_2_en_horas',
                                 'codigo_3_en_horas', 'codigo_4_en_horas', 'codigo_5_en_horas',
                                 'codigo_6_en_horas', 'codigo_7_en_horas', 'codigo_8_en_horas',
                                 'codigo_9_en_horas', 'codigo_10_en_horas', 'codigo_11_en_horas',
                                 'codigo_12_en_horas', 'codigo_13_en_horas', 'codigo_14_en_horas',
                                 'codigo_15_en_horas', 'codigo_16_en_horas', 'codigo_17_en_horas',
                                 'codigo_18_en_horas', 'tiempo_de_paro']
                
                if col in numeric_columns:
                    try:
                        col_cleaned = col_series.astype(str).str.replace(r'\s*(pacas|hrs|horas)', '', regex=True)
                        col_cleaned = col_cleaned.str.replace(r'[^\d.-]', '', regex=True)
                        df[col] = pd.to_numeric(col_cleaned, errors='coerce').fillna(0)
                    except Exception as e:
                        df[col] = 0
                        
                elif col == 'fecha':
                    df[col] = pd.to_datetime(col_series, errors='coerce')
                    
                else:
                    # Para otras columnas, tratar como texto
                    df[col] = col_series.fillna('').astype(str)
            
            # 4. ELIMINAR COLUMNAS DUPLICADAS
            df = self.eliminar_columnas_duplicadas(df)
            
            # 5. Resetear índice
            df = df.reset_index(drop=True)
            
            self.dataframe = df
            print(f"\n✅ Datos limpiados: {df.shape[0]} filas, {df.shape[1]} columnas")
            
            # Mostrar columnas disponibles después de la limpieza
            self.mostrar_columnas_disponibles(df)
            
            return True
            
        except Exception as e:
            logger.error(f"Error limpiando datos: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def crear_dataframe_seguro(self, df, columnas_requeridas, nombre_tabla):
        """Crea un DataFrame de forma segura, manejando columnas faltantes"""
        print(f"\n📊 Creando tabla: {nombre_tabla}")
        
        # Filtrar solo las columnas que existen
        columnas_existentes = [col for col in columnas_requeridas if col in df.columns]
        columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
        
        if columnas_faltantes:
            print(f"   ⚠️  Columnas faltantes: {columnas_faltantes}")
            print(f"   ✅ Columnas disponibles: {columnas_existentes}")
        
        if not columnas_existentes:
            print(f"   ❌ No hay columnas disponibles para {nombre_tabla}")
            return None
        
        # Crear DataFrame con las columnas existentes
        df_temp = df[columnas_existentes].copy()
        
        # Agregar columnas faltantes con valores por defecto
        for col in columnas_faltantes:
            if col in ['turno_inicio', 'turno_final', 'turno_cierre']:
                df_temp[col] = ''  # Texto vacío para turnos
            elif any(x in col for x in ['horas', 'pacas', 'codigo', 'tiempo']):
                df_temp[col] = 0  # Cero para valores numéricos
            else:
                df_temp[col] = ''  # Texto vacío por defecto
        
        # Reordenar columnas según el orden requerido
        columnas_finales = []
        for col in columnas_requeridas:
            if col in df_temp.columns:
                columnas_finales.append(col)
        
        df_final = df_temp[columnas_finales]
        print(f"   ✅ Registros: {len(df_final)}")
        print(f"   ✅ Columnas: {len(columnas_finales)}/{len(columnas_requeridas)}")
        
        return df_final

    def crear_tablas_especificas(self):
        """Crea las 6 tablas específicas a partir de los datos limpios"""
        try:
            if self.dataframe is None or self.dataframe.empty:
                logger.error("No hay datos para crear las tablas")
                return False
            
            df = self.dataframe
            tablas_creadas = {}
            
            print(f"\n" + "="*70)
            print("CREANDO TABLAS ESPECÍFICAS")
            print("="*70)
            
            # 1. TABLA: Produccion_maquina
            columnas_produccion_maquina = [
                'fecha', 'mes', 'maquina', 'pacas_producidas', 'horas_trabajadas', 
                'tiempo_de_paro', 'turno'
            ]
            
            produccion_maquina = self.crear_dataframe_seguro(df, columnas_produccion_maquina, "Produccion_maquina")
            if produccion_maquina is not None:
                # Separar turno
                produccion_maquina['turno_inicio'] = produccion_maquina['turno'].astype(str).str.split('-').str[0]
                produccion_maquina['turno_final'] = produccion_maquina['turno'].astype(str).str.split('-').str[1]
                
                # Columnas finales
                columnas_finales = [
                    'fecha', 'mes', 'maquina', 'pacas_producidas', 'horas_trabajadas',
                    'tiempo_de_paro', 'turno_inicio', 'turno_final'
                ]
                produccion_maquina_final = produccion_maquina[[col for col in columnas_finales if col in produccion_maquina.columns]]
                tablas_creadas['Produccion_maquina'] = produccion_maquina_final
            
            # 2. TABLA: Produccion_operario
            columnas_produccion_operario = [
                'fecha', 'mes', 'maquina', 'operario', 'referencia', 'pacas_producidas',
                'horas_trabajadas', 'turno'
            ]
            
            produccion_operario = self.crear_dataframe_seguro(df, columnas_produccion_operario, "Produccion_operario")
            if produccion_operario is not None:
                # Separar turno
                produccion_operario['turno_inicio'] = produccion_operario['turno'].astype(str).str.split('-').str[0]
                produccion_operario['turno_final'] = produccion_operario['turno'].astype(str).str.split('-').str[1]
                
                # Columnas finales
                columnas_finales = [
                    'fecha', 'mes', 'maquina', 'operario', 'referencia', 'pacas_producidas',
                    'horas_trabajadas', 'turno_inicio', 'turno_final'
                ]
                produccion_operario_final = produccion_operario[[col for col in columnas_finales if col in produccion_operario.columns]]
                tablas_creadas['Produccion_operario'] = produccion_operario_final
            
            # 3. TABLA: Produccion_01
            columnas_produccion_01 = [
                'fecha', 'maquina', 'mes', 'operario', 'pacas_producidas', 'horas_trabajadas',
                'turno', 'horas_no_trabajadas', 'codigo_de_paro_1', 'sub_codigo_de_paro_1',
                'tiempo_de_paro'
            ]
            
            produccion_01 = self.crear_dataframe_seguro(df, columnas_produccion_01, "Produccion_01")
            if produccion_01 is not None:
                # Separar turno
                produccion_01['turno_inicio'] = produccion_01['turno'].astype(str).str.split('-').str[0]
                produccion_01['turno_cierre'] = produccion_01['turno'].astype(str).str.split('-').str[1]
                
                # Columnas finales
                columnas_finales = [
                    'fecha', 'maquina', 'mes', 'operario', 'pacas_producidas', 'horas_trabajadas',
                    'turno_inicio', 'turno_cierre', 'horas_no_trabajadas', 'codigo_de_paro_1',
                    'sub_codigo_de_paro_1', 'tiempo_de_paro'
                ]
                produccion_01_final = produccion_01[[col for col in columnas_finales if col in produccion_01.columns]]
                tablas_creadas['Produccion_01'] = produccion_01_final
            
            # 4. TABLA: Produccion_03
            columnas_produccion_03 = [
                'fecha', 'mes', 'maquina', 'operario', 'pacas_producidas', 'horas_trabajadas',
                'horas_no_trabajadas', 'codigo_de_paro_3', 'subcodigo_3', 'tiempo_de_paro', 'turno'
            ]
            
            produccion_03 = self.crear_dataframe_seguro(df, columnas_produccion_03, "Produccion_03")
            if produccion_03 is not None:
                # Separar turno
                produccion_03['turno_inicio'] = produccion_03['turno'].astype(str).str.split('-').str[0]
                produccion_03['turno_final'] = produccion_03['turno'].astype(str).str.split('-').str[1]
                
                # Columnas finales
                columnas_finales = [
                    'fecha', 'mes', 'maquina', 'operario', 'pacas_producidas', 'horas_trabajadas',
                    'horas_no_trabajadas', 'codigo_de_paro_3', 'subcodigo_3', 'tiempo_de_paro',
                    'turno_inicio', 'turno_final'
                ]
                produccion_03_final = produccion_03[[col for col in columnas_finales if col in produccion_03.columns]]
                tablas_creadas['Produccion_03'] = produccion_03_final
            
            # 5. TABLA: Produccion_05
            columnas_produccion_05 = [
                'fecha', 'mes', 'maquina', 'operario', 'pacas_producidas', 'horas_trabajadas',
                'horas_no_trabajadas', 'codigo_de_paro_5', 'subcodigo_5', 'codigo_5_en_horas',
                'area_involucrada_en_subcodigo_5', 'tiempo_de_paro', 'turno'
            ]
            
            produccion_05 = self.crear_dataframe_seguro(df, columnas_produccion_05, "Produccion_05")
            if produccion_05 is not None:
                # Separar turno
                produccion_05['turno_inicio'] = produccion_05['turno'].astype(str).str.split('-').str[0]
                produccion_05['turno_final'] = produccion_05['turno'].astype(str).str.split('-').str[1]
                
                # Columnas finales
                columnas_finales = [
                    'fecha', 'mes', 'maquina', 'operario', 'pacas_producidas', 'horas_trabajadas',
                    'horas_no_trabajadas', 'codigo_de_paro_5', 'subcodigo_5', 'codigo_5_en_horas',
                    'area_involucrada_en_subcodigo_5', 'tiempo_de_paro', 'turno_inicio', 'turno_final'
                ]
                produccion_05_final = produccion_05[[col for col in columnas_finales if col in produccion_05.columns]]
                tablas_creadas['Produccion_05'] = produccion_05_final
            
            # 6. TABLA: Porcentaje_Codigo_paro
            columnas_porcentaje = [
                'codigo_de_paro_1', 'sub_codigo_de_paro_1', 'codigo_1_en_horas',
                'codigo_de_paro_2', 'codigo_2_en_horas', 'codigo_de_paro_3', 'subcodigo_3',
                'codigo_3_en_horas', 'codigo_de_paro_4', 'codigo_4_en_horas',
                'codigo_de_paro_5', 'subcodigo_5', 'codigo_5_en_horas',
                'area_involucrada_en_subcodigo_5', 'personal_involucrado',
                'codigo_de_paro_6', 'codigo_6_en_horas', 'codigo_de_paro_7', 'codigo_7_en_horas',
                'codigo_de_paro_8', 'codigo_8_en_horas', 'codigo_de_paro_9', 'codigo_9_en_horas',
                'codigo_de_paro_10', 'codigo_10_en_horas', 'codigo_de_paro_11', 'codigo_11_en_horas',
                'codigo_de_paro_12', 'codigo_12_en_horas', 'codigo_de_paro_13', 'codigo_13_en_horas',
                'codigo_de_paro_14', 'codigo_14_en_horas', 'codigo_de_paro_15', 'codigo_15_en_horas',
                'codigo_de_paro_16', 'codigo_16_en_horas', 'codigo_de_paro_17', 'codigo_17_en_horas',
                'codigo_de_paro_18', 'codigo_18_en_horas', 'tiempo_de_paro', 'observaciones'
            ]
            
            porcentaje_codigo_paro = self.crear_dataframe_seguro(df, columnas_porcentaje, "Porcentaje_Codigo_paro")
            if porcentaje_codigo_paro is not None:
                tablas_creadas['Porcentaje_Codigo_paro'] = porcentaje_codigo_paro
            
            print(f"\n📊 RESUMEN TABLAS CREADAS: {len(tablas_creadas)}/6 tablas")
            for tabla_nombre, tabla_df in tablas_creadas.items():
                print(f"   ✅ {tabla_nombre}: {len(tabla_df)} registros, {len(tabla_df.columns)} columnas")
            
            return tablas_creadas if tablas_creadas else None
            
        except Exception as e:
            logger.error(f"Error creando tablas específicas: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def cargar_tablas_mysql(self, tablas):
        """Carga las tablas específicas a MySQL"""
        try:
            if not self.engine:
                logger.error("No hay conexión a MySQL")
                return False
            
            if not tablas:
                logger.error("No hay tablas para cargar")
                return False
            
            print(f"\n" + "="*70)
            print("CARGA DE TABLAS A MYSQL")
            print("="*70)
            
            tablas_cargadas = 0
            
            for nombre_tabla, dataframe in tablas.items():
                if dataframe.empty:
                    print(f"   ⚠️  {nombre_tabla}: Tabla vacía, omitiendo...")
                    continue
                
                try:
                    # Verificar que no hay columnas duplicadas
                    if dataframe.columns.duplicated().any():
                        dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
                    
                    # Cargar a MySQL
                    dataframe.to_sql(
                        name=nombre_tabla.lower(),
                        con=self.engine,
                        if_exists='replace',
                        index=False,
                        chunksize=1000
                    )
                    
                    print(f"   ✅ {nombre_tabla}: {len(dataframe)} registros cargados")
                    tablas_cargadas += 1
                    
                    # Mostrar estructura de la tabla cargada
                    self.mostrar_estructura_tabla(nombre_tabla.lower())
                    
                except Exception as e:
                    logger.error(f"❌ Error cargando tabla {nombre_tabla}: {e}")
            
            print(f"\n📊 Resumen: {tablas_cargadas}/{len(tablas)} tablas cargadas exitosamente")
            return tablas_cargadas > 0
            
        except Exception as e:
            logger.error(f"Error en carga de tablas a MySQL: {e}")
            return False

    def mostrar_estructura_tabla(self, table_name):
        """Muestra la estructura de una tabla en MySQL"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                columns = result.fetchall()
                
                print(f"       🏗️  Estructura: {len(columns)} columnas")
                # Mostrar solo las primeras 5 columnas para no saturar
                for i, col in enumerate(columns[:5]):
                    print(f"         - {col[0]:<20} {col[1]}")
                if len(columns) > 5:
                    print(f"         ... y {len(columns) - 5} columnas más")
                
        except Exception as e:
            print(f"       ⚠️  No se pudo obtener estructura: {e}")

    def run_etl(self):
        """Ejecuta el ETL completo"""
        print("="*70)
        print("ETL ESPECIALIZADO - SEGUIMIENTO TEMPERAS Y VINILOS (6 TABLAS)")
        print("="*70)
        
        # 1. Buscar archivo
        if not self.excel_file_path:
            print("🔍 Buscando archivo Excel...")
        
        if not self.validate_file_path():
            return False
        
        # 2. Conectar a MySQL
        if not self.connect_to_mysql():
            return False
        
        # 3. Leer Excel con estructura compleja
        if not self.read_excel_with_complex_headers():
            return False
        
        # 4. Limpiar y transformar datos
        if not self.clean_and_transform_data():
            return False
        
        # 5. Crear tablas específicas
        tablas = self.crear_tablas_especificas()
        if not tablas:
            return False
        
        # 6. Cargar tablas a MySQL
        if not self.cargar_tablas_mysql(tablas):
            return False
        
        print("\n🎉 ETL COMPLETADO EXITOSAMENTE!")
        print("="*70)
        print("📊 TABLAS CREADAS EN MYSQL:")
        for tabla in tablas.keys():
            print(f"   ✅ {tabla.lower()}")
        
        return True

def main():
    parser = argparse.ArgumentParser(description='ETL para Temperas y Vinilos - 6 TABLAS ROBUSTO')
    parser.add_argument('--excel-file', help='Ruta del archivo Excel')
    parser.add_argument('--db-host', default='localhost', help='Host de MySQL')
    parser.add_argument('--db-user', help='Usuario de MySQL')
    parser.add_argument('--db-password', help='Contraseña de MySQL')
    parser.add_argument('--db-name', default='TEMPERAS', help='Nombre de la BD')
    
    args = parser.parse_args()
    
    db_config = {
        'host': args.db_host,
        'database': args.db_name
    }
    
    if not args.db_user:
        args.db_user = input("Usuario de MySQL: ")
    
    if not args.db_password:
        args.db_password = getpass.getpass("Contraseña de MySQL: ")
    
    db_config['user'] = args.db_user
    db_config['password'] = args.db_password
    
    etl = TemperasVinilosETL(
        excel_file_path=args.excel_file,
        db_config=db_config
    )
    
    success = etl.run_etl()
    
    if success:
        print("\n" + "="*70)
        print("✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("="*70)
        print("📊 Los datos han sido distribuidos en 6 tablas específicas")
        print("🔍 Revisa los logs para detalles de cada tabla")
    else:
        print("\n❌ EL PROCESO FALLÓ")
        print("📋 Revisa los mensajes anteriores para más información")

if __name__ == "__main__":
    main()