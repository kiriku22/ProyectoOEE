# etl_temperas_vinilos_cli.py
import argparse
import getpass
from etl_structured import TemperasVinilosETL

def run_etl_from_cli():
    """
    CLI para ejecutar el ETL de SEGUIMIENTO TEMPERAS Y VINILOS
    """
    parser = argparse.ArgumentParser(
        description='ETL para SEGUIMIENTO TEMPERAS Y VINILOS (CLI + Structured)'
    )

    # Argumentos CLI
    parser.add_argument(
        '--excel-file',
        help='Ruta del archivo Excel (por defecto buscará automáticamente)'
    )
    parser.add_argument(
        '--db-host',
        default='localhost',
        help='Host de MySQL (por defecto: localhost)'
    )
    parser.add_argument(
        '--db-user',
        help='Usuario de MySQL'
    )
    parser.add_argument(
        '--db-password',
        help='Contraseña de MySQL (si no se especifica, se pedirá de forma segura)'
    )
    parser.add_argument(
        '--db-name',
        default='seguimiento_temperas',
        help='Nombre de la base de datos (por defecto: seguimiento_temperas)'
    )

    args = parser.parse_args()

    # Si no hay usuario ni contraseña, pedirlos de forma interactiva
    if not args.db_user:
        args.db_user = input("👤 Usuario de MySQL: ")

    if not args.db_password:
        args.db_password = getpass.getpass("🔒 Contraseña de MySQL: ")

    # Configuración de la BD
    db_config = {
        'host': args.db_host,
        'user': args.db_user,
        'password': args.db_password,
        'database': args.db_name
    }

    # Inicializar el ETL con parámetros del CLI
    etl = TemperasVinilosETL(
        excel_file_path=args.excel_file,
        db_config=db_config
    )

    # Ejecutar proceso ETL completo
    success = etl.run_etl()

    if success:
        print("\n🎉 ¡PROCESO ETL COMPLETADO EXITOSAMENTE!")
        print("="*60)
        print(f"📊 Los datos se cargaron en la base de datos '{args.db_name}'")
        print("📋 Tabla creada: seguimiento_temperas_vinilos")
        print("📁 Log disponible en: etl_process.log")
    else:
        print("\n❌ ERROR EN EL PROCESO ETL")
        print("📄 Revisa el archivo etl_process.log para más información")

if __name__ == "__main__":
    run_etl_from_cli()