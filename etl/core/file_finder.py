from pathlib import Path
import logging
from typing import Optional
from config.settings import ETLConfig

logger = logging.getLogger(__name__)

class FileFinder:
    def __init__(self, config: ETLConfig):
        self.config = config
    
    def find_excel_file(self, excel_file_path: Optional[str] = None) -> Optional[str]:
        """Busca automáticamente el archivo Excel en el proyecto"""
        if excel_file_path and Path(excel_file_path).exists():
            return excel_file_path
        
        try:
            current_dir = Path.cwd()
            
            for pattern in self.config.excel_patterns:
                files = list(current_dir.glob(pattern))
                for file in files:
                    if file.exists():
                        logger.info(f"📁 Archivo encontrado: {file}")
                        return str(file)
            
            logger.error("❌ No se encontraron archivos Excel en el proyecto")
            return None
            
        except Exception as e:
            logger.error(f"Error buscando archivo Excel: {e}")
            return None
    
    def validate_file_path(self, file_path: str) -> bool:
        """Valida y corrige la ruta del archivo"""
        path = Path(file_path)
        
        if not path.is_absolute():
            path = Path.cwd() / path
        
        return path.exists()