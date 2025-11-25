import logging
from typing import Dict

logger = logging.getLogger(__name__)

class ParoProcessor:
    def __init__(self, total_codigos: int = 18):
        self.total_codigos = total_codigos
    
    def generate_paro_expressions(self) -> Dict[str, str]:
        """Genera expresiones SQL para procesar códigos de paro"""
        expresiones_minutos = []
        expresiones_codigos = []
        selects_estadisticas = []
        sumas_minutos = []
        
        for i in range(1, self.total_codigos + 1):
            expr_minutos = self._generate_minutos_expression(i)
            expr_codigos = self._generate_codigos_expression(i)
            
            expresiones_minutos.append(expr_minutos)
            expresiones_codigos.append(expr_codigos)
            selects_estadisticas.append(f"SUM(CASE WHEN codigo_paro_{i} IS NOT NULL THEN 1 ELSE 0 END) as paros_{i}")
            sumas_minutos.append(f"SUM(minutos_paro_{i}) as total_minutos_{i}")
        
        return {
            'minutos': ',\n            '.join(expresiones_minutos),
            'codigos': ',\n            '.join(expresiones_codigos),
            'estadisticas': ',\n            '.join(selects_estadisticas),
            'sumas_minutos': ',\n            '.join(sumas_minutos)
        }
    
    def _generate_minutos_expression(self, index: int) -> str:
        return f"""
        CASE 
            WHEN `Codigo_{index}_en_horas` IS NOT NULL AND `Codigo_{index}_en_horas` != '' 
            THEN CAST(REGEXP_REPLACE(`Codigo_{index}_en_horas`, '[^0-9.]', '') AS DECIMAL(10,2))
            ELSE 0 
        END AS minutos_paro_{index}"""
    
    def _generate_codigos_expression(self, index: int) -> str:
        return f"""
        CASE 
            WHEN `Codigo_de_paro_{index}` IS NOT NULL AND `Codigo_de_paro_{index}` != '' 
            THEN '{index}'
            ELSE NULL 
        END AS codigo_paro_{index}"""