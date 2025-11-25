import logging
import pandas as pd
from core.database import DatabaseManager

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def load_raw_data(self, dataframe: pd.DataFrame, table_name: str = "datos_crudos_temperas_vinilos") -> bool:
        """Carga los datos crudos a MySQL"""
        try:
            if dataframe is None or dataframe.empty:
                logger.error("No hay datos para cargar")
                return False
            
            dataframe.to_sql(
                name=table_name,
                con=self.db.engine,
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            
            logger.info(f"✅ Tabla '{table_name}' creada exitosamente")
            logger.info(f"📊 Total de registros: {len(dataframe)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos crudos: {e}")
            return False