import logging
from typing import Dict, List, Any
from models.entities import ColumnMapping

logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, total_codigos: int = 18):
        self.total_codigos = total_codigos
    
    def map_columns(self, actual_columns: List[str]) -> ColumnMapping:
        """Mapea columnas esperadas vs reales"""
        mapping_dict = {}
        
        # Mapear columnas básicas
        basic_columns = {
            'fecha': 'fecha',
            'mes': 'mes', 
            'año': 'año',
            'maquina': 'maquina',
            'operario': 'operario',
            'referencia': 'referencia',
            'pacas_producidas': 'pacas_producidas',
            'horas_trabajadas': 'horas_trabajadas',
            'horas_no_trabajadas': 'horas_no_trabajadas',
            'tiempo_de_paro': 'tiempo_de_paro',
            'turno': 'turno',
            'sub_codigo_de_paro_1': 'sub_codigo_de_paro_1',
            'subcodigo_3': 'subcodigo_3',
            'subcodigo_5': 'subcodigo_5',
            'area_involucrada_en_subcodigo_5': 'area_involucrada_en_subcodigo_5',
            'personal_involucrado': 'personal_involucrado',
            'observaciones': 'observaciones'
        }
        
        for expected, key in basic_columns.items():
            mapping_dict[key] = self._find_exact_column(expected, actual_columns)
        
        # Mapear códigos de paro dinámicamente
        codigos_en_horas = {}
        codigos_de_paro = {}
        
        for i in range(1, self.total_codigos + 1):
            codigo_horas_key = f'codigo_{i}_en_horas'
            codigo_paro_key = f'codigo_de_paro_{i}'
            
            codigos_en_horas[i] = self._find_exact_column(codigo_horas_key, actual_columns)
            codigos_de_paro[i] = self._find_exact_column(codigo_paro_key, actual_columns)
        
        mapping_dict['codigos_en_horas'] = codigos_en_horas
        mapping_dict['codigos_de_paro'] = codigos_de_paro
        
        return ColumnMapping(**mapping_dict)
    
    def _find_exact_column(self, pattern: str, columns: List[str]) -> str:
        """Encuentra una columna exacta por patrón"""
        pattern_lower = pattern.lower()
        for col in columns:
            if pattern_lower in col.lower():
                return col
        return None
    
    def generate_sql_expression(self, column_name: str, mapping: ColumnMapping, is_numeric: bool = False) -> str:
        """Genera expresión SQL para una columna"""
        col_real = None
        
        # Para columnas básicas
        if hasattr(mapping, column_name):
            col_real = getattr(mapping, column_name)
        # Para códigos de paro en horas
        elif column_name.startswith('codigo_') and '_en_horas' in column_name:
            try:
                codigo_num = int(column_name.split('_')[1])
                col_real = mapping.codigos_en_horas.get(codigo_num)
            except (ValueError, IndexError):
                col_real = None
        # Para códigos de paro
        elif column_name.startswith('codigo_de_paro_'):
            try:
                codigo_num = int(column_name.split('_')[-1])
                col_real = mapping.codigos_de_paro.get(codigo_num)
            except (ValueError, IndexError):
                col_real = None
        
        if col_real:
            if is_numeric:
                return f"CAST(REGEXP_REPLACE(`{col_real}`, '[^0-9.]', '') AS DECIMAL(10,2))"
            else:
                return f"`{col_real}`"
        else:
            if is_numeric:
                return "0"
            else:
                return "NULL"