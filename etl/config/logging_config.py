import logging
import sys

def setup_logging(log_file: str = 'etl_process.log') -> logging.Logger:
    """Configura el sistema de logging"""
    logger = logging.getLogger('etl_temperas_vinilos')
    logger.setLevel(logging.INFO)
    
    # Formato común
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger