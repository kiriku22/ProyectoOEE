import logging
from sqlalchemy import text
from core.database import DatabaseManager
from transform.data_cleaner import DataCleaner
from transform.paro_processor import ParoProcessor
from models.entities import ColumnMapping

logger = logging.getLogger(__name__)

class SQLProcessor:
    def __init__(self, db_manager: DatabaseManager, total_codigos: int = 18):
        self.db = db_manager
        self.cleaner = DataCleaner(total_codigos)
        self.paro_processor = ParoProcessor(total_codigos)
    
    def execute_cleaning_queries(self) -> bool:
        """Ejecuta queries SQL para limpiar y transformar los datos"""
        try:
            with self.db.engine.connect() as conn:
                # Obtener estructura de la tabla
                actual_columns = self.db.get_table_columns("datos_crudos_temperas_vinilos")
                mapping = self.cleaner.map_columns(actual_columns)
                
                # Crear tabla limpia
                self._create_clean_table(conn, mapping)
                
                # Procesar códigos de paro
                self._process_paro_codes(conn)
                
                # Crear tablas específicas
                self._create_specific_tables(conn)
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Error ejecutando queries SQL: {e}")
            return False
    
    def _create_clean_table(self, conn, mapping: ColumnMapping):
        """Crea la tabla con datos limpios"""
        query = self._build_clean_table_query(mapping)
        conn.execute(text(query))
        logger.info("✅ Tabla 'datos_limpios_temperas_vinilos' creada")
        
        # Contar registros
        result = conn.execute(text("SELECT COUNT(*) FROM datos_limpios_temperas_vinilos"))
        count = result.fetchone()[0]
        logger.info(f"📊 Registros en tabla limpia: {count}")
    
    def _build_clean_table_query(self, mapping: ColumnMapping) -> str:
        """Construye el query para crear la tabla limpia"""
        query = f"""
        CREATE TABLE IF NOT EXISTS datos_limpios_temperas_vinilos AS
        SELECT 
            -- Columnas básicas
            {self.cleaner.generate_sql_expression('fecha', mapping)} AS fecha,
            {self.cleaner.generate_sql_expression('mes', mapping)} AS mes,
            {self.cleaner.generate_sql_expression('año', mapping)} AS año,
            {self.cleaner.generate_sql_expression('maquina', mapping)} AS maquina,
            {self.cleaner.generate_sql_expression('operario', mapping)} AS operario,
            {self.cleaner.generate_sql_expression('referencia', mapping)} AS referencia,
            
            -- Extraer números de texto
            {self.cleaner.generate_sql_expression('pacas_producidas', mapping, True)} AS pacas_producidas,
            {self.cleaner.generate_sql_expression('horas_trabajadas', mapping, True)} AS horas_trabajadas,
            {self.cleaner.generate_sql_expression('horas_no_trabajadas', mapping, True)} AS horas_no_trabajadas,
            {self.cleaner.generate_sql_expression('tiempo_de_paro', mapping, True)} AS tiempo_de_paro,
            
            -- Separar turno en inicio y final
            SUBSTRING_INDEX({self.cleaner.generate_sql_expression('turno', mapping)}, '-', 1) AS turno_inicio,
            SUBSTRING_INDEX({self.cleaner.generate_sql_expression('turno', mapping)}, '-', -1) AS turno_final"""
        
        # Agregar columnas de códigos dinámicamente (1-18)
        for i in range(1, self.cleaner.total_codigos + 1):
            query += f""",
            -- Códigos de paro {i} (preservar texto original)
            {self.cleaner.generate_sql_expression(f'codigo_{i}_en_horas', mapping)} AS Codigo_{i}_en_horas,
            {self.cleaner.generate_sql_expression(f'codigo_de_paro_{i}', mapping)} AS Codigo_de_paro_{i}"""
        
        # Agregar columnas adicionales
        query += f""",
            
            -- Textos originales adicionales
            {self.cleaner.generate_sql_expression('sub_codigo_de_paro_1', mapping)} AS sub_codigo_de_paro_1,
            {self.cleaner.generate_sql_expression('subcodigo_3', mapping)} AS subcodigo_3,
            {self.cleaner.generate_sql_expression('subcodigo_5', mapping)} AS subcodigo_5,
            {self.cleaner.generate_sql_expression('area_involucrada_en_subcodigo_5', mapping)} AS area_involucrada_en_subcodigo_5,
            {self.cleaner.generate_sql_expression('personal_involucrado', mapping)} AS personal_involucrado,
            {self.cleaner.generate_sql_expression('observaciones', mapping)} AS observaciones
            
        FROM datos_crudos_temperas_vinilos;
        """
        
        return query
    
    def _process_paro_codes(self, conn):
        """Procesa los códigos de paro"""
        logger.info("🔄 Procesando códigos de paro...")
        
        expressions = self.paro_processor.generate_paro_expressions()
        
        # Crear tabla temporal
        temp_query = f"""
        CREATE TABLE IF NOT EXISTS temp_codigos_paro AS
        SELECT *,
            {expressions['minutos']},
            {expressions['codigos']}
        FROM datos_limpios_temperas_vinilos;
        """
        conn.execute(text(temp_query))
        logger.info("✅ Tabla temporal 'temp_codigos_paro' creada")
        
        # Crear tabla final procesada
        final_query = self._build_final_paro_table_query()
        conn.execute(text(final_query))
        logger.info("✅ Tabla 'datos_paros_procesados' creada")
        
        # Mostrar estadísticas
        self._show_paro_statistics(conn)
        
        # Limpiar tabla temporal
        conn.execute(text("DROP TABLE IF EXISTS temp_codigos_paro"))
        logger.info("✅ Tabla temporal eliminada")
    
    def _build_final_paro_table_query(self) -> str:
        """Construye query para tabla final de paros"""
        query = """
        CREATE TABLE IF NOT EXISTS datos_paros_procesados AS
        SELECT 
            -- Columnas básicas
            fecha, mes, año, maquina, operario, referencia,
            pacas_producidas, horas_trabajadas, horas_no_trabajadas, tiempo_de_paro,
            turno_inicio, turno_final,
            
            -- Códigos de paro procesados (números) y minutos"""
        
        # Agregar columnas dinámicas para códigos 1-18
        for i in range(1, self.cleaner.total_codigos + 1):
            query += f",\n            codigo_paro_{i}, minutos_paro_{i}"
        
        # Agregar información adicional de paros
        query += """,
            
            -- Información adicional de paros preservada
            sub_codigo_de_paro_1, subcodigo_3, subcodigo_5,
            area_involucrada_en_subcodigo_5, personal_involucrado, observaciones
            
        FROM temp_codigos_paro;
        """
        
        return query
    
    def _show_paro_statistics(self, conn):
        """Muestra estadísticas de paros procesados"""
        expressions = self.paro_processor.generate_paro_expressions()
        
        stats_query = f"""
        SELECT 
            COUNT(*) as total_registros,
            {expressions['estadisticas']},
            {expressions['sumas_minutos']}
        FROM datos_paros_procesados;
        """
        
        result = conn.execute(text(stats_query))
        stats = result.fetchone()
        
        logger.info(f"📊 ESTADÍSTICAS DE PAROS PROCESADOS (1-18):")
        logger.info(f"   Total registros: {stats[0]}")
        
        # Mostrar estadísticas para cada código
        total_minutos_general = 0
        for i in range(1, self.cleaner.total_codigos + 1):
            paros_count = stats[i]  # índice 1-18 para conteos
            minutos_total = stats[self.cleaner.total_codigos + i]  # índice 19-36 para minutos
            total_minutos_general += minutos_total
            if paros_count > 0:
                logger.info(f"   Paros código {i}: {paros_count} registros (Total minutos: {minutos_total})")
        
        logger.info(f"   🔴 TOTAL MINUTOS PARO: {total_minutos_general}")
        
        # Mostrar ejemplos de datos procesados
        logger.info(f"🔍 EJEMPLOS DE DATOS PROCESADOS:")
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
            logger.info(f"   Ejemplo {i}:")
            for j in range(0, 6, 2):
                codigo = ejemplo[j]
                minutos = ejemplo[j+1]
                if codigo is not None:
                    logger.info(f"     - Código {codigo}: {minutos} minutos")
    
    def _create_specific_tables(self, conn):
        """Crea las tablas específicas del negocio"""
        tables_queries = {
            'produccion_maquina': self._get_produccion_maquina_query(),
            'produccion_operario': self._get_produccion_operario_query(),
            'analisis_paros': self._get_analisis_paros_query()
        }
        
        for table_name, query in tables_queries.items():
            conn.execute(text(query))
            logger.info(f"✅ Tabla '{table_name}' creada")
        
        # Crear tablas adicionales básicas
        tablas_adicionales = [
            ('produccion_01', "CREATE TABLE IF NOT EXISTS produccion_01 AS SELECT * FROM datos_limpios_temperas_vinilos WHERE 1=0"),
            ('produccion_03', "CREATE TABLE IF NOT EXISTS produccion_03 AS SELECT * FROM datos_limpios_temperas_vinilos WHERE 1=0"),
            ('produccion_05', "CREATE TABLE IF NOT EXISTS produccion_05 AS SELECT * FROM datos_limpios_temperas_vinilos WHERE 1=0"),
            ('porcentaje_codigo_paro', "CREATE TABLE IF NOT EXISTS porcentaje_codigo_paro AS SELECT * FROM datos_paros_procesados WHERE 1=0")
        ]
        
        for nombre_tabla, query in tablas_adicionales:
            try:
                conn.execute(text(query))
                logger.info(f"✅ Tabla '{nombre_tabla}' creada (estructura básica)")
            except Exception as e:
                logger.error(f"❌ No se pudo crear '{nombre_tabla}': {e}")
        
        # Mostrar resumen de tablas creadas
        self._show_tables_summary(conn)
    
    def _get_produccion_maquina_query(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS produccion_maquina AS
        SELECT 
            fecha, mes, maquina, 
            COALESCE(pacas_producidas, 0) AS pacas_producidas,
            COALESCE(horas_trabajadas, 0) AS horas_trabajadas,
            COALESCE(tiempo_de_paro, 0) AS tiempo_de_paro,
            turno_inicio, turno_final
        FROM datos_limpios_temperas_vinilos;
        """
    
    def _get_produccion_operario_query(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS produccion_operario AS
        SELECT 
            fecha, mes, maquina, operario, referencia,
            COALESCE(pacas_producidas, 0) AS pacas_producidas,
            COALESCE(horas_trabajadas, 0) AS horas_trabajadas,
            turno_inicio, turno_final
        FROM datos_limpios_temperas_vinilos;
        """
    
    def _get_analisis_paros_query(self) -> str:
        """Construye query para tabla de análisis de paros"""
        query = """
        CREATE TABLE IF NOT EXISTS analisis_paros AS
        SELECT 
            fecha, mes, maquina, operario"""
        
        # Agregar columnas dinámicas para códigos 1-18
        for i in range(1, self.cleaner.total_codigos + 1):
            query += f",\n                    codigo_paro_{i}, minutos_paro_{i}"
        
        # Calcular total de minutos
        suma_minutos = " + ".join([f"COALESCE(minutos_paro_{i}, 0)" for i in range(1, self.cleaner.total_codigos + 1)])
        query += f",\n                    ({suma_minutos}) as total_minutos_paro"
        query += "\n                FROM datos_paros_procesados;"
        
        return query
    
    def _show_tables_summary(self, conn):
        """Muestra resumen de todas las tablas creadas"""
        tablas_creadas = [
            'datos_crudos_temperas_vinilos', 
            'datos_limpios_temperas_vinilos', 
            'datos_paros_procesados',
            'produccion_maquina',
            'produccion_operario', 
            'analisis_paros',
            'produccion_01',
            'produccion_03',
            'produccion_05',
            'porcentaje_codigo_paro'
        ]
        
        logger.info(f"📊 RESUMEN DE TABLAS CREADAS:")
        for table in tablas_creadas:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                logger.info(f"   ✅ {table}: {count} registros")
            except Exception as e:
                logger.info(f"   ⚠️  {table}: no se pudo contar - {e}")