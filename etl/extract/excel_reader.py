import pandas as pd
import re
import logging
from typing import Tuple, Optional
from models.entities import ExcelFileInfo, ColumnMapping

logger = logging.getLogger(__name__)

class ExcelReader:
    def __init__(self):
        self.dataframe = None
    
    def read_excel_raw(self, file_path: str) -> Tuple[bool, Optional[pd.DataFrame]]:
        """Lee el archivo Excel sin transformaciones"""
        try:
            logger.info(f"📖 Leyendo archivo Excel: {file_path}")
            excel_file = pd.ExcelFile(file_path)
            
            sheet_name = self._find_target_sheet(excel_file.sheet_names)
            data_start_row = self._find_data_start_row(file_path, sheet_name)
            
            # Leer datos con header correcto
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=data_start_row)
            df.columns = [self._clean_column_name_basic(col) for col in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]
            
            logger.info(f"✅ Datos leídos: {df.shape[0]} filas × {df.shape[1]} columnas")
            self.dataframe = df
            return True, df
            
        except Exception as e:
            logger.error(f"❌ Error leyendo Excel: {e}")
            return False, None
    
    def _find_target_sheet(self, sheet_names: list) -> str:
        """Encuentra la hoja objetivo"""
        for sheet_name in sheet_names:
            if 'base de datos' in sheet_name.lower():
                logger.info(f"✅ Hoja encontrada: {sheet_name}")
                return sheet_name
        
        target_sheet = sheet_names[0]
        logger.warning(f"⚠️  Usando hoja: {target_sheet}")
        return target_sheet
    
    def _find_data_start_row(self, file_path: str, sheet_name: str) -> int:
        """Encuentra la fila donde empiezan los datos reales"""
        df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=10)
        
        for i in range(len(df_raw)):
            row = df_raw.iloc[i]
            if self._is_data_header_row(row):
                return i
        return 0
    
    def _is_data_header_row(self, row) -> bool:
        """Determina si una fila es el encabezado de datos"""
        row_str = ' '.join([str(x) for x in row if pd.notna(x)])
        patterns = [
            'fecha', 'mes', 'año', 'maquina', 'operario', 'referencia',
            'unidad', 'display', 'paca', 'horas', 'turno', 'paro'
        ]
        row_lower = row_str.lower()
        matches = sum(1 for pattern in patterns if pattern in row_lower)
        return matches >= 3
    
    def _clean_column_name_basic(self, column_name) -> str:
        """Limpia nombres de columnas básico para SQL"""
        if pd.isna(column_name):
            return "columna_desconocida"
        
        col_name = str(column_name).strip().lower()
        col_name = re.sub(r'[^\w]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name)
        col_name = col_name.strip('_')
        
        return col_name or "columna_desconocida"