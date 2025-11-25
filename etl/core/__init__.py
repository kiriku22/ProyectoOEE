"""
Módulo core con componentes fundamentales del ETL
"""
from .database import DatabaseManager
from .file_finder import FileFinder

__all__ = ['DatabaseManager', 'FileFinder']