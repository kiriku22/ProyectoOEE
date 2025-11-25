# etl_python_sql_hibrido.py
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

    def read_excel_raw(self):
        """Lee el archivo Excel SIN transformaciones - solo lectura básica"""
        try:
            if not self.validate_file_path():
                logger.error("No se pudo encontrar el archivo Excel")
                return False
            
            logger.info(f"📖 Leyendo archivo Excel: {self.excel_file_path}")
            
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
            
            # Leer datos SIN header para análisis
            df_raw = pd.read_excel(self.excel_file_path, sheet_name=target_sheet, header=None, nrows=10)
            
            print(f"\n🔍 Analizando estructura del archivo...")
            print("Primeras 10 filas crudas:")
            print(df_raw.to_string())
            
            # Encontrar la fila donde empiezan los datos reales
            data_start_row = self.find_data_start_row(df_raw)
            print(f"\n📊 Fila donde inician los datos: {data_start_row}")
            
            # Leer datos con header correcto pero SIN transformaciones
            df = pd.read_excel(self.excel_file_path, sheet_name=target_sheet, header=data_start_row)
            
            # Solo limpiar nombres de columnas básico
            df.columns = [self.clean_column_name_basic(col) for col in df.columns]
            
            # Eliminar duplicados de columnas
            df = df.loc[:, ~df.columns.duplicated()]
            
            self.dataframe = df
            print(f"\n✅ Datos leídos: {df.shape[0]} filas × {df.shape[1]} columnas")
            print("📋 Columnas detectadas:")
            for i, col in enumerate(df.columns, 1):
                print(f"  {i:2d}. {col}")
            
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

    def clean_column_name_basic(self, column_name):
        """Limpia nombres de columnas básico para SQL"""
        if pd.isna(column_name):
            return "columna_desconocida"
        
        col_name = str(column_name).strip().lower()
        col_name = re.sub(r'[^\w]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name)
        col_name = col_name.strip('_')
        
        return col_name or "columna_desconocida"

    def cargar_datos_crudos_mysql(self):
        """Carga los datos crudos a MySQL para procesamiento con SQL"""
        try:
            if self.dataframe is None or self.dataframe.empty:
                logger.error("No hay datos para cargar")
                return False
            
            print(f"\n" + "="*70)
            print("CARGA DE DATOS CRUDOS A MYSQL")
            print("="*70)
            
            table_name = "datos_crudos_temperas_vinilos"
            
            # Cargar datos crudos a MySQL
            self.dataframe.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            
            print(f"✅ Tabla '{table_name}' creada exitosamente")
            print(f"📊 Total de registros: {len(self.dataframe)}")
            print(f"🏗️  Total de columnas: {len(self.dataframe.columns)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos crudos: {e}")
            return False

    def generar_expresiones_codigos_paro(self, total_codigos=18):
        """Genera expresiones SQL para procesar hasta 18 códigos de paro"""
        expresiones_minutos = []
        expresiones_codigos = []
        selects_estadisticas = []
        sumas_minutos = []
        
        for i in range(1, total_codigos + 1):
            # Expresiones para extraer minutos de las celdas de tiempo
            expr_minutos = f"""
            CASE 
                WHEN `Codigo_{i}_en_horas` IS NOT NULL AND `Codigo_{i}_en_horas` != '' 
                THEN CAST(REGEXP_REPLACE(`Codigo_{i}_en_horas`, '[^0-9.]', '') AS DECIMAL(10,2))
                ELSE 0 
            END AS minutos_paro_{i}"""
            expresiones_minutos.append(expr_minutos)
            
            # Expresiones para extraer el número del código de paro
            # Si hay contenido en la celda de código, usar el número correspondiente
            expr_codigos = f"""
            CASE 
                WHEN `Codigo_de_paro_{i}` IS NOT NULL AND `Codigo_de_paro_{i}` != '' 
                THEN '{i}'  -- Reemplazar con el número del código
                ELSE NULL 
            END AS codigo_paro_{i}"""
            expresiones_codigos.append(expr_codigos)
            
            # Para estadísticas
            selects_estadisticas.append(f"SUM(CASE WHEN codigo_paro_{i} IS NOT NULL THEN 1 ELSE 0 END) as paros_{i}")
            sumas_minutos.append(f"SUM(minutos_paro_{i}) as total_minutos_{i}")
        
        return {
            'minutos': ',\n            '.join(expresiones_minutos),
            'codigos': ',\n            '.join(expresiones_codigos),
            'estadisticas': ',\n            '.join(selects_estadisticas),
            'sumas_minutos': ',\n            '.join(sumas_minutos)
        }

    def procesar_codigos_paro(self, conn, mapeo_columnas):
        """Procesa los códigos de paro - separa código (número) de minutos"""
        print(f"\n🔄 Procesando códigos de paro (1-18)...")
        
        # Verificar las columnas reales en la tabla limpia
        print(f"🔍 Verificando columnas disponibles en la tabla limpia...")
        result = conn.execute(text("SHOW COLUMNS FROM datos_limpios_temperas_vinilos"))
        columnas_limpias = [row[0] for row in result.fetchall()]
        
        print(f"📋 Columnas disponibles en tabla limpia:")
        columnas_codigos = [col for col in columnas_limpias if 'codigo' in col.lower()]
        for col in columnas_codigos[:10]:
            print(f"  - {col}")
        if len(columnas_codigos) > 10:
            print(f"  - ... ({len(columnas_codigos) - 10} columnas más)")
        
        # Generar expresiones SQL dinámicamente
        expresiones = self.generar_expresiones_codigos_paro(18)
        
        # Crear tabla temporal para procesar códigos de paro
        temp_table_query = f"""
        CREATE TABLE IF NOT EXISTS temp_codigos_paro AS
        SELECT *,
            -- Extraer minutos de las columnas de códigos en horas
            {expresiones['minutos']},
            
            -- Extraer solo el número del código de paro 
            -- Si hay contenido en la celda, usar el número correspondiente
            {expresiones['codigos']}
            
        FROM datos_limpios_temperas_vinilos;
        """
        
        try:
            conn.execute(text(temp_table_query))
            print("✅ Tabla temporal 'temp_codigos_paro' creada")
        except Exception as e:
            print(f"❌ Error creando tabla temporal: {e}")
            print(f"🔍 Columnas disponibles en datos_limpios_temperas_vinilos:")
            for col in columnas_limpias:
                if any(f'codigo_{i}' in col.lower() for i in range(1, 19)):
                    print(f"  - {col}")
            return False
        
        # Crear tabla final con los códigos de paro procesados
        final_table_query = """
        CREATE TABLE IF NOT EXISTS datos_paros_procesados AS
        SELECT 
            -- Columnas básicas
            fecha, mes, año, maquina, operario, referencia,
            pacas_producidas, horas_trabajadas, horas_no_trabajadas, tiempo_de_paro,
            turno_inicio, turno_final,
            
            -- Códigos de paro procesados (números) y minutos"""
        
        # Agregar columnas dinámicas para códigos 1-18
        for i in range(1, 19):
            final_table_query += f",\n            codigo_paro_{i}, minutos_paro_{i}"
        
        # Agregar información adicional de paros
        final_table_query += """,
            
            -- Información adicional de paros preservada
            sub_codigo_de_paro_1, subcodigo_3, subcodigo_5,
            area_involucrada_en_subcodigo_5, personal_involucrado, observaciones
            
        FROM temp_codigos_paro;
        """
        
        conn.execute(text(final_table_query))
        print("✅ Tabla 'datos_paros_procesados' creada")
        
        # Mostrar estadísticas de paros procesados
        stats_query = f"""
        SELECT 
            COUNT(*) as total_registros,
            {expresiones['estadisticas']},
            {expresiones['sumas_minutos']}
        FROM datos_paros_procesados;
        """
        
        result = conn.execute(text(stats_query))
        stats = result.fetchone()
        
        print(f"\n📊 ESTADÍSTICAS DE PAROS PROCESADOS (1-18):")
        print(f"   Total registros: {stats[0]}")
        
        # Mostrar estadísticas para cada código
        for i in range(1, 19):
            paros_count = stats[i]  # índice 1-18 para conteos
            minutos_total = stats[18 + i]  # índice 19-36 para minutos
            if paros_count > 0:
                print(f"   Paros código {i}: {paros_count} registros (Total minutos: {minutos_total})")
        
        # Calcular total general de minutos
        total_minutos_general = sum(stats[19:37])
        print(f"   🔴 TOTAL MINUTOS PARO: {total_minutos_general}")
        
        # Mostrar ejemplos de datos procesados
        print(f"\n🔍 EJEMPLOS DE DATOS PROCESADOS:")
        ejemplo_query = """
        SELECT 
            codigo_paro_1, minutos_paro_1,
            codigo_paro_2, minutos_paro_2,
            codigo_paro_3, minutos_paro_3
        FROM datos_paros_procesados 
        WHERE codigo_paro_1 IS NOT NULL OR codigo_paro_2 IS NOT NULL OR codigo_paro_3 IS NOT NULL
        LIMIT 5;
        """
        
        result = conn.execute(text(ejemplo_query))
        ejemplos = result.fetchall()
        
        for i, ejemplo in enumerate(ejemplos, 1):
            print(f"   Ejemplo {i}:")
            for j in range(0, 6, 2):
                codigo = ejemplo[j]
                minutos = ejemplo[j+1]
                if codigo is not None:
                    print(f"     - Código {codigo}: {minutos} minutos")
        
        # Limpiar tabla temporal
        conn.execute(text("DROP TABLE IF EXISTS temp_codigos_paro"))
        print("✅ Tabla temporal eliminada")

    def ejecutar_queries_limpieza(self):
        """Ejecuta queries SQL para limpiar y transformar los datos"""
        try:
            print(f"\n" + "="*70)
            print("EJECUTANDO QUERIES DE LIMPIEZA EN SQL")
            print("="*70)
            
            with self.engine.connect() as conn:
                
                # Primero, obtener los nombres reales de las columnas
                print(f"\n🔍 Obteniendo estructura de la tabla cruda...")
                result = conn.execute(text("SHOW COLUMNS FROM datos_crudos_temperas_vinilos"))
                columnas_reales = [row[0] for row in result.fetchall()]
                
                print(f"📋 Columnas reales en la tabla: {len(columnas_reales)}")
                for i, col in enumerate(columnas_reales, 1):
                    print(f"  {i:2d}. {col}")
                
                # Función para mapear nombres de columnas
                def encontrar_columna_exacta(patron, columnas):
                    patron_lower = patron.lower()
                    for col in columnas:
                        if patron_lower in col.lower():
                            return col
                    return None
                
                # Mapear columnas esperadas vs reales
                mapeo_columnas = {}
                columnas_esperadas = [
                    'fecha', 'mes', 'año', 'maquina', 'operario', 'referencia',
                    'pacas_producidas', 'horas_trabajadas', 'horas_no_trabajadas', 'tiempo_de_paro',
                    'turno'
                ]
                
                # Agregar columnas para códigos 1-18
                for i in range(1, 19):
                    columnas_esperadas.extend([
                        f'codigo_{i}_en_horas',
                        f'codigo_de_paro_{i}'
                    ])
                
                # Agregar columnas adicionales
                columnas_esperadas.extend([
                    'sub_codigo_de_paro_1', 'subcodigo_3', 'subcodigo_5',
                    'area_involucrada_en_subcodigo_5', 'personal_involucrado', 'observaciones'
                ])
                
                print(f"\n🔄 Mapeando columnas...")
                columnas_encontradas = 0
                for col_esperada in columnas_esperadas:
                    col_real = encontrar_columna_exacta(col_esperada, columnas_reales)
                    if col_real:
                        mapeo_columnas[col_esperada] = col_real
                        columnas_encontradas += 1
                        if 'codigo' in col_esperada and any(str(i) in col_esperada for i in range(1, 6)):
                            print(f"  ✅ '{col_esperada}' -> '{col_real}'")
                    else:
                        mapeo_columnas[col_esperada] = None
                        if 'codigo' in col_esperada and any(str(i) in col_esperada for i in range(1, 6)):
                            print(f"  ⚠️  '{col_esperada}' -> NO ENCONTRADA")
                
                print(f"\n📊 Resumen mapeo: {columnas_encontradas}/{len(columnas_esperadas)} columnas encontradas")
                
                # Función para generar la expresión SQL
                def generar_expresion_sql(nombre_columna, mapeo, es_numerica=False):
                    col_real = mapeo.get(nombre_columna)
                    if col_real:
                        if es_numerica:
                            return f"CAST(REGEXP_REPLACE(`{col_real}`, '[^0-9.]', '') AS DECIMAL(10,2))"
                        else:
                            return f"`{col_real}`"
                    else:
                        if es_numerica:
                            return "0"
                        else:
                            return "NULL"
                
                # 1. Crear tabla limpia
                print(f"\n🔄 Creando tabla con datos limpios...")
                
                create_clean_table_query = f"""
                CREATE TABLE IF NOT EXISTS datos_limpios_temperas_vinilos AS
                SELECT 
                    -- Columnas básicas
                    {generar_expresion_sql('fecha', mapeo_columnas)} AS fecha,
                    {generar_expresion_sql('mes', mapeo_columnas)} AS mes,
                    {generar_expresion_sql('año', mapeo_columnas)} AS año,
                    {generar_expresion_sql('maquina', mapeo_columnas)} AS maquina,
                    {generar_expresion_sql('operario', mapeo_columnas)} AS operario,
                    {generar_expresion_sql('referencia', mapeo_columnas)} AS referencia,
                    
                    -- Extraer números de texto
                    {generar_expresion_sql('pacas_producidas', mapeo_columnas, True)} AS pacas_producidas,
                    {generar_expresion_sql('horas_trabajadas', mapeo_columnas, True)} AS horas_trabajadas,
                    {generar_expresion_sql('horas_no_trabajadas', mapeo_columnas, True)} AS horas_no_trabajadas,
                    {generar_expresion_sql('tiempo_de_paro', mapeo_columnas, True)} AS tiempo_de_paro,
                    
                    -- Separar turno en inicio y final
                    SUBSTRING_INDEX({generar_expresion_sql('turno', mapeo_columnas)}, '-', 1) AS turno_inicio,
                    SUBSTRING_INDEX({generar_expresion_sql('turno', mapeo_columnas)}, '-', -1) AS turno_final"""
                
                # Agregar columnas de códigos dinámicamente (1-18)
                for i in range(1, 19):
                    create_clean_table_query += f""",
                    -- Códigos de paro {i} (preservar texto original)
                    {generar_expresion_sql(f'codigo_{i}_en_horas', mapeo_columnas)} AS Codigo_{i}_en_horas,
                    {generar_expresion_sql(f'codigo_de_paro_{i}', mapeo_columnas)} AS Codigo_de_paro_{i}"""
                
                # Agregar columnas adicionales
                create_clean_table_query += f""",
                    
                    -- Textos originales adicionales
                    {generar_expresion_sql('sub_codigo_de_paro_1', mapeo_columnas)} AS sub_codigo_de_paro_1,
                    {generar_expresion_sql('subcodigo_3', mapeo_columnas)} AS subcodigo_3,
                    {generar_expresion_sql('subcodigo_5', mapeo_columnas)} AS subcodigo_5,
                    {generar_expresion_sql('area_involucrada_en_subcodigo_5', mapeo_columnas)} AS area_involucrada_en_subcodigo_5,
                    {generar_expresion_sql('personal_involucrado', mapeo_columnas)} AS personal_involucrado,
                    {generar_expresion_sql('observaciones', mapeo_columnas)} AS observaciones
                    
                FROM datos_crudos_temperas_vinilos;
                """
                
                conn.execute(text(create_clean_table_query))
                print("✅ Tabla 'datos_limpios_temperas_vinilos' creada")
                
                # Contar registros en tabla limpia
                result = conn.execute(text("SELECT COUNT(*) FROM datos_limpios_temperas_vinilos"))
                count = result.fetchone()[0]
                print(f"📊 Registros en tabla limpia: {count}")
                
                # Mostrar estructura de la tabla limpia
                print(f"\n🔍 Estructura de la tabla limpia (primeros códigos):")
                result = conn.execute(text("SHOW COLUMNS FROM datos_limpios_temperas_vinilos"))
                columnas_count = 0
                for row in result.fetchall():
                    if 'codigo' in row[0].lower() and any(str(i) in row[0].lower() for i in range(1, 6)):
                        print(f"  - {row[0]} ({row[1]})")
                        columnas_count += 1
                
                print(f"  - ... ({36 - columnas_count} columnas más de códigos 6-18)")
                
                # PROCESAR CÓDIGOS DE PARO - NUEVA LÓGICA
                self.procesar_codigos_paro(conn, mapeo_columnas)
                
                # Crear las tablas específicas
                print(f"\n🔄 Creando tablas específicas...")
                
                tablas_creadas = ['datos_crudos_temperas_vinilos', 'datos_limpios_temperas_vinilos', 'datos_paros_procesados']
                
                # Tabla: Produccion_maquina
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS produccion_maquina AS
                SELECT 
                    fecha, mes, maquina, 
                    COALESCE(pacas_producidas, 0) AS pacas_producidas,
                    COALESCE(horas_trabajadas, 0) AS horas_trabajadas,
                    COALESCE(tiempo_de_paro, 0) AS tiempo_de_paro,
                    turno_inicio, turno_final
                FROM datos_limpios_temperas_vinilos;
                """))
                tablas_creadas.append('produccion_maquina')
                print("✅ Tabla 'produccion_maquina' creada")
                
                # Tabla: Produccion_operario
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS produccion_operario AS
                SELECT 
                    fecha, mes, maquina, operario, referencia,
                    COALESCE(pacas_producidas, 0) AS pacas_producidas,
                    COALESCE(horas_trabajadas, 0) AS horas_trabajadas,
                    turno_inicio, turno_final
                FROM datos_limpios_temperas_vinilos;
                """))
                tablas_creadas.append('produccion_operario')
                print("✅ Tabla 'produccion_operario' creada")
                
                # Tabla: Analisis_paros (usando los datos procesados)
                analisis_query = """
                CREATE TABLE IF NOT EXISTS analisis_paros AS
                SELECT 
                    fecha, mes, maquina, operario"""
                
                # Agregar columnas dinámicas para códigos 1-18
                for i in range(1, 19):
                    analisis_query += f",\n                    codigo_paro_{i}, minutos_paro_{i}"
                
                # Calcular total de minutos
                suma_minutos = " + ".join([f"COALESCE(minutos_paro_{i}, 0)" for i in range(1, 19)])
                analisis_query += f",\n                    ({suma_minutos}) as total_minutos_paro"
                analisis_query += "\n                FROM datos_paros_procesados;"
                
                conn.execute(text(analisis_query))
                tablas_creadas.append('analisis_paros')
                print("✅ Tabla 'analisis_paros' creada")
                
                # Tablas adicionales básicas
                tablas_adicionales = [
                    ('produccion_01', "CREATE TABLE IF NOT EXISTS produccion_01 AS SELECT * FROM datos_limpios_temperas_vinilos WHERE 1=0"),
                    ('produccion_03', "CREATE TABLE IF NOT EXISTS produccion_03 AS SELECT * FROM datos_limpios_temperas_vinilos WHERE 1=0"),
                    ('produccion_05', "CREATE TABLE IF NOT EXISTS produccion_05 AS SELECT * FROM datos_limpios_temperas_vinilos WHERE 1=0"),
                    ('porcentaje_codigo_paro', "CREATE TABLE IF NOT EXISTS porcentaje_codigo_paro AS SELECT * FROM datos_paros_procesados WHERE 1=0")
                ]
                
                for nombre_tabla, query in tablas_adicionales:
                    try:
                        conn.execute(text(query))
                        tablas_creadas.append(nombre_tabla)
                        print(f"✅ Tabla '{nombre_tabla}' creada (estructura básica)")
                    except Exception as e:
                        print(f"❌ No se pudo crear '{nombre_tabla}': {e}")
                
                # Mostrar resumen de tablas creadas
                print(f"\n📊 RESUMEN DE TABLAS CREADAS:")
                for table in tablas_creadas:
                    try:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        count = result.fetchone()[0]
                        print(f"   ✅ {table}: {count} registros")
                    except:
                        print(f"   ⚠️  {table}: no se pudo contar")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Error ejecutando queries SQL: {e}")
            return False

    def run_etl(self):
        """Ejecuta el ETL híbrido Python + SQL"""
        print("="*70)
        print("ETL HÍBRIDO - PYTHON + SQL")
        print("="*70)
        print("🎯 ESTRATEGIA: Python lee datos + SQL los transforma")
        print("⚡ PROCESAMIENTO: 18 códigos de paro (separación código/minutos)")
        print("🆕 NUEVA LÓGICA: Si hay contenido → código = número, minutos = valor")
        print("="*70)
        
        # 1. Buscar archivo
        if not self.excel_file_path:
            print("🔍 Buscando archivo Excel...")
        
        if not self.validate_file_path():
            return False
        
        # 2. Conectar a MySQL
        if not self.connect_to_mysql():
            return False
        
        # 3. Leer Excel
        if not self.read_excel_raw():
            return False
        
        # 4. Cargar datos crudos a MySQL
        if not self.cargar_datos_crudos_mysql():
            return False
        
        # 5. Ejecutar lógica de transformación en SQL
        if not self.ejecutar_queries_limpieza():
            return False
        
        print("\n🎉 ETL HÍBRIDO COMPLETADO EXITOSAMENTE!")
        print("="*70)
        print("📊 RESUMEN FINAL:")
        print("   ✅ SEPARACIÓN EXITOSA: Códigos vs Minutos")
        print("   ✅ LÓGICA IMPLEMENTADA: Si hay contenido → código = número")
        print("   ✅ MINUTOS PRESERVADOS: Valores numéricos extraídos correctamente")
        print("   ✅ TABLAS CREADAS: datos_paros_procesados con estructura separada")
        print("="*70)
        
        return True

def main():
    parser = argparse.ArgumentParser(description='ETL Híbrido Python + SQL - SEPARACIÓN CÓDIGOS/MINUTOS')
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
        print("🎯 SEPARACIÓN CÓDIGOS/MINUTOS IMPLEMENTADA:")
        print("   📝 Si 'codigo_de_paro_1' tiene contenido → 'codigo_paro_1' = '1'")
        print("   ⏱️  Si 'Codigo_1_en_horas' tiene '20 mnts' → 'minutos_paro_1' = 20.0")
        print("   🔄 Para todos los códigos 1-18")
        print("   📊 Ejemplo: Código 1: 20 minutos, Código 2: 15 minutos, etc.")
        print("="*70)
    else:
        print("\n❌ EL PROCESO FALLÓ")
        print("📋 Revisa los mensajes anteriores para más información")

if __name__ == "__main__":
    main()