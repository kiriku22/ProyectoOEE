import os
import pandas as pd
import mysql.connector

# 🔹 Conexión a MySQL (ajusta según tu configuración)
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="MAQUINARIA_PINTURAS"
)

cursor = conn.cursor()

# 🔹 Carpeta donde están los CSV (dentro del proyecto)
csv_folder = "database"

# 🔹 Mapeo de archivos CSV a tablas y columnas
table_mappings = {
    "produccion_01.csv": {
        "table": "produccion_01",
        "columns": [
            "fecha", "maquina", "mes", "operario", "pacas_producidas", 
            "horas_trabajadas", "turno_inicio", "turno_cierre", 
            "horas_no_trabajadas", "codigo_paro_1", "sub_codigo_paro_1", "tiempo_paro"
        ]
    },
    "ProduccionMaquinaria.csv": {
        "table": "ProduccionMaquinaria",
        "columns": [
            "fecha", "mes", "maquina", "pacas_producidas", 
            "horas_trabajadas", "tiempo_paro", "turno_inicio", "turno_final"
        ]
    },
    "ProduccionOperario.csv": {
        "table": "ProduccionOperario",
        "columns": [
            "fecha", "mes", "maquina", "operario", "referencia", 
            "pacas_producidas", "horas_trabajadas", "turno_inicio", "turno_final"
        ]
    },
    "produccion_03.csv": {
        "table": "produccion_03",
        "columns": [
            "fecha", "mes", "maquina", "operario", "pacas_producidas", 
            "horas_trabajadas", "horas_no_trabajadas", "codigo_paro_3", 
            "subcodigo_3", "tiempo_paro", "turno_inicio", "turno_final"
        ]
    },
    "produccion_05.csv": {
        "table": "produccion_05",
        "columns": [
            "fecha", "mes", "maquina", "operario", "pacas_producidas", 
            "horas_trabajadas", "horas_no_trabajadas", "codigo_paro_5", 
            "subcodigo_5", "codigo_5_en_horas", "area_involucrada_subcodigo_5", 
            "tiempo_paro", "turno_inicio", "turno_final"
        ]
    }
}

# 🔹 Iterar sobre todos los archivos CSV
for file in os.listdir(csv_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(csv_folder, file)
        print(f"📂 Procesando archivo: {file}")
        
        # Verificar si el archivo tiene mapeo definido
        if file not in table_mappings:
            print(f"⚠️  No hay mapeo definido para {file}, saltando...\n")
            continue
        
        mapping = table_mappings[file]
        table_name = mapping["table"]
        columns = mapping["columns"]
        
        try:
            # Leer CSV con pandas
            df = pd.read_csv(file_path)
            
            # Limpiar nombres de columnas (eliminar espacios extras)
            df.columns = df.columns.str.strip()
            
            print(f"📊 Columnas detectadas en CSV: {list(df.columns)}")
            print(f"🏷️  Columnas esperadas para tabla: {columns}")
            
            # Verificar que las columnas necesarias estén presentes
            missing_columns = set(columns) - set(df.columns)
            if missing_columns:
                print(f"❌ Columnas faltantes en CSV: {missing_columns}")
                continue
            
            # Preparar placeholders para la consulta SQL
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            
            # Contador para seguimiento
            inserted_rows = 0
            
            # Insertar datos fila por fila
            for _, row in df.iterrows():
                try:
                    # Preparar valores, manejando valores NaN
                    values = []
                    for col in columns:
                        value = row[col]
                        if pd.isna(value):
                            values.append(None)
                        else:
                            values.append(value)
                    
                    # Ejecutar inserción
                    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                    cursor.execute(query, tuple(values))
                    inserted_rows += 1
                    
                except Exception as e:
                    print(f"❌ Error insertando fila: {e}")
                    print(f"   Valores: {values}")
                    continue
            
            # Confirmar transacción
            conn.commit()
            print(f"✅ Archivo {file} importado correctamente. Filas insertadas: {inserted_rows}\n")
            
        except Exception as e:
            print(f"❌ Error procesando archivo {file}: {e}\n")
            conn.rollback()

cursor.close()
conn.close()
print("🚀 Proceso de importación completado.")