from dataclasses import dataclass
from typing import Optional, List, Dict
from datetime import datetime

@dataclass
class DatabaseConfig:
    host: str
    user: str
    password: str
    database: str

@dataclass
class ExcelFileInfo:
    path: str
    sheet_name: str
    data_start_row: int

@dataclass
class ColumnMapping:
    fecha: Optional[str] = None
    mes: Optional[str] = None
    año: Optional[str] = None
    maquina: Optional[str] = None
    operario: Optional[str] = None
    referencia: Optional[str] = None
    pacas_producidas: Optional[str] = None
    horas_trabajadas: Optional[str] = None
    horas_no_trabajadas: Optional[str] = None
    tiempo_de_paro: Optional[str] = None
    turno: Optional[str] = None
    
    # Códigos de paro - usar Dict en lugar de List individual
    codigos_en_horas: Optional[Dict[int, str]] = None
    codigos_de_paro: Optional[Dict[int, str]] = None
    
    # Columnas adicionales
    sub_codigo_de_paro_1: Optional[str] = None
    subcodigo_3: Optional[str] = None
    subcodigo_5: Optional[str] = None
    area_involucrada_en_subcodigo_5: Optional[str] = None
    personal_involucrado: Optional[str] = None
    observaciones: Optional[str] = None
    
    def __post_init__(self):
        # Inicializar los diccionarios si son None
        if self.codigos_en_horas is None:
            self.codigos_en_horas = {}
        if self.codigos_de_paro is None:
            self.codigos_de_paro = {}