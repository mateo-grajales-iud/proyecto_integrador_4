import os
import requests
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from pandas import DataFrame, read_csv, read_json, to_datetime
from sqlalchemy import create_engine
from typing import Dict

# Constantes
DB_PATH = os.path.expanduser("~/airflow/sqlite/")
DB_NAME = "database.db"
CSV_PATH = os.path.expanduser("~/airflow/datasets/")
HOLIDAYS_NAME = "holidays.json"
CONN = None

CSV_TO_TABLE_MAPPING = dict([
            ("olist_customers_dataset.csv", "olist_customers"),
            ("olist_geolocation_dataset.csv", "olist_geolocation"),
            ("olist_order_items_dataset.csv", "olist_order_items"),
            ("olist_order_payments_dataset.csv", "olist_order_payments"),
            ("olist_order_reviews_dataset.csv", "olist_order_reviews"),
            ("olist_orders_dataset.csv", "olist_orders"),
            ("olist_products_dataset.csv", "olist_products"),
            ("olist_sellers_dataset.csv", "olist_sellers"),
            (
                "product_category_name_translation.csv",
                "product_category_name_translation",
            ),
        ])

def get_public_holidays() -> DataFrame | None:
    holidays_path = os.path.join(CSV_PATH, HOLIDAYS_NAME)
    columnas_esperadas = [
        "date", "localName", "name", "countryCode", 
        "fixed", "global", "launchYear"
    ]
    try:
        print(f"📂 Intentando leer archivo de feriados: {holidays_path}")
        data = read_json(holidays_path)
        df = DataFrame(data)
        df.drop(columns=["types", "counties"], inplace=True, errors="ignore")
        df["date"] = to_datetime(df["date"], format="%Y-%m-%d")
        print("✅ Archivo de feriados cargado exitosamente.")
        return df
    except FileNotFoundError:
        print(f"⚠️ Archivo no encontrado. Se creará DataFrame vacío con columnas: {columnas_esperadas}")
    except Exception as e:
        print(f"❌ Error al leer '{holidays_path}': {e}")
    
    return DataFrame(columns=columnas_esperadas)

# Paso 1: Verificar si la BD existe
def check_bd():
    # Crear el directorio si no existe
    if not os.path.exists(DB_PATH):
        os.makedirs(DB_PATH)
        print(f"📁 Carpeta creada: {DB_PATH}")

    db_full_path = os.path.join(DB_PATH, DB_NAME)

    # Verificar si existe el archivo .db
    if os.path.isfile(db_full_path):
        # Borrar la base de datos existente
        os.remove(db_full_path)
        print(f"🗑️ Base de datos existente eliminada: {db_full_path}")

    # Crear una nueva base de datos vacía
    CONN = sqlite3.connect(db_full_path)
    print(f"✅ Nueva base de datos creada: {db_full_path}")
    CONN.close()

# Paso 2: Crear tablas e insertar datos desde los CSV
def create_tables():
    print("Creando tablas e insertando datos desde CSVs...")
    dataframes = {}
    for csv_file, table_name in CSV_TO_TABLE_MAPPING.items():
        csv_path = os.path.join(CSV_PATH, csv_file)

        if not os.path.exists(csv_path):
            print(f"⚠️ Archivo CSV no encontrado: {csv_path}. Saltando '{table_name}'...")
            continue

        try:
            print(f"📄 Leyendo CSV para la tabla '{table_name}' desde: {csv_path}")
            df = read_csv(csv_path)
            dataframes[table_name] = df
        except Exception as e:
            print(f"❌ Error al leer '{csv_path}': {e}")
            continue

    holidays = get_public_holidays()
    dataframes["public_holidays"] = holidays
    CONN = sqlite3.connect(DB_PATH + DB_NAME)
    for table_name, df in dataframes.items():
        try:
            print(f"📝 Guardando DataFrame en la tabla '{table_name}'...")
            df.to_sql(name=table_name, con=CONN, if_exists='replace', index=False)
            print(f"✅ Tabla '{table_name}' creada con {len(df)} registros.")
        except Exception as e:
            print(f"❌ Error al guardar la tabla '{table_name}': {e}")

# DAG
with DAG(
    dag_id="extract_csv_to_db",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["csv", "sqlite", "etl"]
) as dag:

    task_check_bd = PythonOperator(
        task_id="check_bd",
        python_callable=check_bd
    )

    task_create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    task_check_bd >> task_create_tables  # Secuencia

