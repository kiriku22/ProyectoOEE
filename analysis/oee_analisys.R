# Conexión a PostgreSQL
library(RPostgreSQL)
library(dplyr)
library(lubridate)

conectar_bd <- function() {
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, 
                   dbname = "oee_database",
                   host = "localhost", 
                   port = 5432,
                   user = "postgres",
                   password = "tu_password")
  return(con)
}

# Función para calcular OEE
calcular_oee <- function(con, fecha_inicio, fecha_fin, maquina_id = NULL) {
  
  # Construir query base
  query_base <- "
  SELECT 
    rp.fecha,
    m.nombre_maquina,
    p.nombre_producto,
    rp.cantidad_producida,
    rp.cantidad_defectuosa,
    p.ciclo_teorico_segundos,
    p.objetivo_produccion_hora,
    EXTRACT(EPOCH FROM (rp.hora_fin - rp.hora_inicio)) / 3600 as horas_totales,
    COALESCE(SUM(fp.duracion_minutos) / 60, 0) as horas_paro
  FROM fact_registro_produccion rp
  JOIN dim_maquina m ON rp.maquina_id = m.maquina_id
  JOIN dim_producto p ON rp.producto_id = p.producto_id
  LEFT JOIN fact_detalle_paros fp ON rp.registro_id = fp.registro_id
  WHERE rp.fecha BETWEEN '%s' AND '%s'
  "
  
  # Agregar filtro por máquina si se especifica
  if (!is.null(maquina_id)) {
    query_base <- paste0(query_base, " AND rp.maquina_id = ", maquina_id)
  }
  
  query_base <- paste0(query_base, "
  GROUP BY rp.fecha, m.nombre_maquina, p.nombre_producto, 
           rp.cantidad_producida, rp.cantidad_defectuosa,
           p.ciclo_teorico_segundos, p.objetivo_produccion_hora,
           rp.hora_inicio, rp.hora_fin
  ")
  
  query <- sprintf(query_base, fecha_inicio, fecha_fin)
  
  datos <- dbGetQuery(con, query)
  
  # Calcular métricas OEE
  datos <- datos %>%
    mutate(
      # Disponibilidad
      horas_operativas = horas_totales - horas_paro,
      disponibilidad = horas_operativas / horas_totales,
      
      # Rendimiento
      producción_ideal = horas_operativas * 3600 / ciclo_teorico_segundos,
      rendimiento = pmin(cantidad_producida / producción_ideal, 1),
      
      # Calidad
      cantidad_buena = cantidad_producida - cantidad_defectuosa,
      calidad = cantidad_buena / cantidad_producida,
      
      # OEE
      oee = disponibilidad * rendimiento * calidad
    )
  
  return(datos)
}

# Ejemplo de uso
con <- conectar_bd()
resultados_oee <- calcular_oee(con, '2024-01-01', '2024-01-31')
print(head(resultados_oee))

# Generar reporte resumen
reporte_oee <- resultados_oee %>%
  group_by(nombre_maquina) %>%
  summarise(
    oee_promedio = mean(oee, na.rm = TRUE),
    disponibilidad_promedio = mean(disponibilidad, na.rm = TRUE),
    rendimiento_promedio = mean(rendimiento, na.rm = TRUE),
    calidad_promedio = mean(calidad, na.rm = TRUE)
  )

print(reporte_oee)