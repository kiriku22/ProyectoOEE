"""
Módulo core con componentes fundamentales del ETL

by: kiriku2
"""
from .database import DatabaseManager
from .file_finder import FileFinder

__all__ = ['DatabaseManager', 'FileFinder']