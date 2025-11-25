from sqlalchemy import create_engine, text
from typing import Optional
import logging
from models.entities import DatabaseConfig

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = None
    
    def connect(self) -> bool:
        """Establece conexión con MySQL"""
        try:
            connection_string = (
                f"mysql+mysqlconnector://{self.config.user}:{self.config.password}"
                f"@{self.config.host}/{self.config.database}"
            )
            self.engine = create_engine(connection_string)
            
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info("✅ Conectado a MySQL")
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando a MySQL: {e}")
            return False
    
    def execute_query(self, query: str) -> bool:
        """Ejecuta una consulta SQL"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Error ejecutando query: {e}")
            return False
    
    def get_table_columns(self, table_name: str) -> list:
        """Obtiene las columnas de una tabla"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"❌ Error obteniendo columnas: {e}")
            return []