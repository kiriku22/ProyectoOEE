# ProyectoOEE
# Visualizacion en grafana
## sudo service grafana-server start
## abrir localhost:3000
# iniciar msyql
## sudo service mysql start


# CONSULTAS SQL

## produccion Operarios:
## SELECT operario, MAX(pacas_producidas) FROM MAQUINARIA_PINTURAS.ProduccionOperario where operario != 'No aplica' GROUP BY operario LIMIT 50 

