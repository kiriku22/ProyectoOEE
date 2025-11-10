import os
import pandas as pd
import mysql.connector

# 🔹 Conexión a MySQL (ajusta según tu configuración)
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root123",
    database="MAQUINARIA_PINTURAS"
)

cursor = conn.cursor()

# 🔹 Carpeta donde están los CSV (dentro del proyecto)
csv_folder = "database"

# 🔹 Mapeo de archivos CSV a tablas y columnas (ACTUALIZADO con tus nombres reales)
table_mappings = {
    "PRODUCION 01.csv": {
        "table": "produccion_01",
        "columns": [
            "fecha", "maquina", "mes", "operario", "pacas_producidas", 
            "horas_trabajadas", "turno_inicio", "turno_cierre", 
            "horas_no_trabajadas", "codigo_paro_1", "sub_codigo_paro_1", "tiempo_paro"
        ]
    },
    "PRODUCCION_MAQUINA.csv": {
        "table": "ProduccionMaquinaria",
        "columns": [
            "fecha", "mes", "maquina", "pacas_producidas", 
            "horas_trabajadas", "tiempo_paro", "turno_inicio", "turno_final"
        ]
    },
    "PRODUCCION_OPERARIOS.csv": {
        "table": "ProduccionOperario",
        "columns": [
            "fecha", "mes", "maquina", "operario", "referencia", 
            "pacas_producidas", "horas_trabajadas", "turno_inicio", "turno_final"
        ]
    },
    "PRODUCCION 03.csv": {
        "table": "produccion_03",
        "columns": [
            "fecha", "mes", "maquina", "operario", "pacas_producidas", 
            "horas_trabajadas", "horas_no_trabajadas", "codigo_paro_3", 
            "subcodigo_3", "tiempo_paro", "turno_inicio", "turno_final"
        ]
    },
    "PRODUCCION 05.csv": {
        "table": "produccion_05",
        "columns": [
            "fecha", "mes", "maquina", "operario", "pacas_producidas", 
            "horas_trabajadas", "horas_no_trabajadas", "codigo_paro_5", 
            "subcodigo_5", "codigo_5_en_horas", "area_involucrada_subcodigo_5", 
            "tiempo_paro", "turno_inicio", "turno_final"
        ]
    },
    "PORCENTAJE_CODIGOS_PARO.csv": {
        "table": "PorcentajeCodigoParo",
        "columns": [
            "codigo_de_paro_1", "sub_codigo_de_paro_1", "codigo_1_en_horas", 
            "codigo_de_paro_2", "codigo_2_en_horas", "codigo_de_paro_3", 
            "subcodigo_3", "codigo_3_en_horas", "codigo_de_paro_4", 
            "codigo_4_en_horas", "codigo_de_paro_5", "subcodigo_5", 
            "codigo_5_en_horas", "area_involucrada_en_subcodigo_5", 
            "personal_involucrado", "codigo_de_paro_6", "codigo_6_en_horas", 
            "codigo_de_paro_7", "codigo_7_en_horas", "codigo_de_paro_8", 
            "codigo_8_en_horas", "codigo_de_paro_9", "codigo_9_en_horas", 
            "codigo_de_paro_10", "codigo_10_en_horas", "codigo_de_paro_11", 
            "codigo_11_en_horas", "codigo_de_paro_12", "codigo_12_en_horas", 
            "codigo_de_paro_13", "codigo_13_en_horas", "codigo_de_paro_14", 
            "codigo_14_en_horas", "codigo_de_paro_15", "codigo_15_en_horas", 
            "codigo_de_paro_16", "codigo_16_en_horas", "codigo_de_paro_17", 
            "codigo_17_en_horas", "codigo_de_paro_18", "codigo_18_en_horas", 
            "tiempo_de_paro", "observaciones"
        ]
    }
}

# 🔹 Función para encontrar archivos ignorando mayúsculas/minúsculas y espacios
def find_matching_file(filename, folder_files):
    # Normalizar el nombre (minúsculas, sin espacios extras)
    normalized_target = filename.lower().replace(" ", "_").replace(".csv", "")
    
    for actual_file in folder_files:
        normalized_actual = actual_file.lower().replace(" ", "_").replace(".csv", "")
        if normalized_target == normalized_actual:
            return actual_file
    return None

# 🔹 Iterar sobre los archivos en la carpeta
folder_files = os.listdir(csv_folder)
csv_files = [f for f in folder_files if f.endswith(".csv")]

print(f"📁 Archivos CSV encontrados: {csv_files}")

for target_file in table_mappings.keys():
    # Buscar archivo que coincida (ignorando mayúsculas/minúsculas y espacios)
    actual_file = find_matching_file(target_file, csv_files)
    
    if actual_file:
        file_path = os.path.join(csv_folder, actual_file)
        print(f"📂 Procesando archivo: {actual_file} (mapeado a {target_file})")
        
        mapping = table_mappings[target_file]
        table_name = mapping["table"]
        columns = mapping["columns"]
        
        try:
            # Leer CSV con pandas
            df = pd.read_csv(file_path)
            
            # Limpiar nombres de columnas (eliminar espacios extras y normalizar)
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
            
            print(f"📊 Columnas detectadas en CSV: {list(df.columns)}")
            print(f"🏷️  Columnas esperadas para tabla: {columns}")
            
            # Verificar que las columnas necesarias estén presentes
            available_columns = list(df.columns)
            missing_columns = [col for col in columns if col not in available_columns]
            
            if missing_columns:
                print(f"⚠️  Columnas faltantes en CSV: {missing_columns}")
                print(f"📋 Usando solo columnas disponibles: {available_columns}")
                # Usar solo las columnas que están disponibles
                available_mapping_columns = [col for col in columns if col in available_columns]
            else:
                available_mapping_columns = columns
            
            if not available_mapping_columns:
                print(f"❌ No hay columnas coincidentes, saltando archivo...")
                continue
            
            # Preparar placeholders para la consulta SQL
            placeholders = ", ".join(["%s"] * len(available_mapping_columns))
            column_names = ", ".join(available_mapping_columns)
            
            # Contador para seguimiento
            inserted_rows = 0
            
            # Insertar datos fila por fila
            for _, row in df.iterrows():
                try:
                    # Preparar valores, manejando valores NaN
                    values = []
                    for col in available_mapping_columns:
                        value = row[col]
                        if pd.isna(value):
                            values.append(None)
                        else:
                            # Convertir a tipo adecuado si es necesario
                            if isinstance(value, (int, float)) and col.endswith('_horas'):
                                values.append(float(value))
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
            print(f"✅ Archivo {actual_file} importado correctamente. Filas insertadas: {inserted_rows}\n")
            
        except Exception as e:
            print(f"❌ Error procesando archivo {actual_file}: {e}\n")
            conn.rollback()
    else:
        print(f"⚠️  No se encontró archivo que coincida con: {target_file}")

cursor.close()
conn.close()
print("🚀 Proceso de importación completado.")