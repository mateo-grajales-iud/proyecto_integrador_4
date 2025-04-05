import os
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Rutas de origen y destino
ORIGEN = os.path.expanduser("~/Documents/datos")
DESTINO = os.path.expanduser("~/airflow/datasets/")

def move_datasets():
    if not os.path.exists(DESTINO):
        os.makedirs(DESTINO)
    
    archivos = [f for f in os.listdir(ORIGEN)]

    for archivo in archivos:
        ruta_origen = os.path.join(ORIGEN, archivo)
        ruta_destino = os.path.join(DESTINO, archivo)

        if os.path.exists(ruta_destino):
            print(f"Ya existe el archivo '{archivo}' en el destino. Se omite.")
            continue

        shutil.move(ruta_origen, ruta_destino)
        print(f"Archivo movido: {archivo}")

with DAG(
    dag_id="move_datasets",
    schedule_interval="*/5 * * * *",  # Cada 5 minutos
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["csv"]
) as dag:

    move_task = PythonOperator(
        task_id="move_datasets",
        python_callable=move_datasets
    )

    trigger_extract = TriggerDagRunOperator(
    task_id="trigger_extract",
    trigger_dag_id="apply_queries",  # nombre exacto del DAG destino
    )

    move_task >> trigger_extract

