from dataclasses import dataclass
from pathlib import Path

@dataclass
class DatabaseConfig:
    host: str = 'localhost'
    user: str = 'root'
    password: str = 'root123'
    database: str = 'TEMPERAS'

@dataclass
class ETLConfig:
    excel_patterns: tuple = (
        "**/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm",
        "**/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsx",
        "**/SEGUIMIENTO TEMPERAS*.xls*",
        "**/*TEMPERAS*.xls*",
        "**/*VINILOS*.xls*"
    )
    total_codigos_paro: int = 18
    log_file: str = 'etl_process.log'

class Config:
    def __init__(self):
        self.db = DatabaseConfig()
        self.etl = ETLConfig()