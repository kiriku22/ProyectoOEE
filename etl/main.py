import argparse
import getpass
from config.logging_config import setup_logging
from config.settings import Config
from core.database import DatabaseManager
from core.file_finder import FileFinder
from extract.excel_reader import ExcelReader
from load.data_loader import DataLoader
from transform.sql_processor import SQLProcessor

class TemperasVinilosETL:
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logging(config.etl.log_file)
        
        self.file_finder = FileFinder(config.etl)
        self.db_manager = DatabaseManager(config.db)
        self.excel_reader = ExcelReader()
        self.data_loader = DataLoader(self.db_manager)
        self.sql_processor = SQLProcessor(self.db_manager, config.etl.total_codigos_paro)
    
    def run_etl(self, excel_file_path: str = None) -> bool:
        """Ejecuta el proceso ETL completo"""
        self.logger.info("🚀 Iniciando ETL Híbrido - Python + SQL")
        
        # 1. Buscar archivo
        file_path = self.file_finder.find_excel_file(excel_file_path)
        if not file_path:
            return False
        
        # 2. Conectar a MySQL
        if not self.db_manager.connect():
            return False
        
        # 3. Leer Excel
        success, dataframe = self.excel_reader.read_excel_raw(file_path)
        if not success:
            return False
        
        # 4. Cargar datos crudos
        if not self.data_loader.load_raw_data(dataframe):
            return False
        
        # 5. Procesar con SQL
        if not self.sql_processor.execute_cleaning_queries():
            return False
        
        self.logger.info("🎉 ETL HÍBRIDO COMPLETADO EXITOSAMENTE!")
        return True

def main():
    parser = argparse.ArgumentParser(description='ETL Híbrido Python + SQL')
    parser.add_argument('--excel-file', help='Ruta del archivo Excel')
    parser.add_argument('--db-host', default='localhost', help='Host de MySQL')
    parser.add_argument('--db-user', help='Usuario de MySQL')
    parser.add_argument('--db-password', help='Contraseña de MySQL')
    parser.add_argument('--db-name', default='TEMPERAS', help='Nombre de la BD')
    
    args = parser.parse_args()
    
    # Configuración
    config = Config()
    config.db.host = args.db_host
    config.db.database = args.db_name
    config.db.user = args.db_user or input("Usuario de MySQL: ")
    config.db.password = args.db_password or getpass.getpass("Contraseña de MySQL: ")
    
    # Ejecutar ETL
    etl = TemperasVinilosETL(config)
    success = etl.run_etl(args.excel_file)
    
    if not success:
        print("\n❌ EL PROCESO FALLÓ")
        print("📋 Revisa los logs para más información")

if __name__ == "__main__":
    main()