from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sqlite3
import pandas as pd
from pandas import DataFrame, read_sql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine.base import Engine

DB_INPUT_PATH = os.path.expanduser("~/airflow/sqlite/database.db")
DB_OUTPUT_PATH = os.path.expanduser("~/airflow/sqlite/output.db")

queries_dict = {
    "delivery_date_difference": """select oc.customer_state as State, CAST(AVG(julianday(strftime('%Y-%m-%d', order_estimated_delivery_date)) - julianday(strftime('%Y-%m-%d', order_delivered_customer_date))) AS INTEGER) as Delivery_Difference from olist_orders oo join olist_customers oc on oo.customer_id = oc.customer_id where oo.order_status == "delivered" and oo.order_delivered_customer_date is not null group by oc.customer_state order by Delivery_Difference""",
    "global_ammount_order_status": """select oo.order_status, count(oo.order_status) as Ammount from olist_orders oo group by oo.order_status""",
    "revenue_by_month_year": """WITH stage AS ( SELECT oo.order_id, oo.customer_id, oo.order_delivered_customer_date, oop.payment_value FROM olist_orders oo JOIN olist_order_payments oop ON oo.order_id = oop.order_id WHERE oo.order_delivered_customer_date IS NOT NULL AND oo.order_status = "delivered" GROUP BY oo.order_id ORDER BY oo.order_delivered_customer_date ) SELECT strftime('%m', s.order_delivered_customer_date) AS month_no, case strftime("%m", s.order_delivered_customer_date) WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar' WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun' WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep' WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec' end as month, SUM(CASE WHEN strftime('%Y', s.order_delivered_customer_date) = '2016' THEN s.payment_value ELSE 0.00 END) AS Year2016, SUM(CASE WHEN strftime('%Y', s.order_delivered_customer_date) = '2017' THEN s.payment_value ELSE 0.00 END) AS Year2017, SUM(CASE WHEN strftime('%Y', s.order_delivered_customer_date) = '2018' THEN s.payment_value ELSE 0.00 END) AS Year2018 FROM stage s GROUP BY month_no, month ORDER BY month_no""",
    "revenue_per_state": """select oc.customer_state as customer_state, sum(oop.payment_value) as Revenue from olist_orders oo join olist_customers oc on oo.customer_id = oc.customer_id join olist_order_payments oop on oo.order_id = oop.order_id where oo.order_status = "delivered" and oo.order_delivered_customer_date is not null group by oc.customer_state order BY Revenue desc limit 10""",
    "top_10_least_revenue_categories": """select pcnt.product_category_name_english Category, count(DISTINCT oo.order_id) Num_order, sum(oop.payment_value) Revenue from olist_orders oo join olist_order_items ooi on oo.order_id = ooi.order_id join olist_order_payments oop on oo.order_id = oop.order_id join olist_products op on ooi.product_id = op.product_id join product_category_name_translation pcnt on op.product_category_name = pcnt.product_category_name where oo.order_status = "delivered" and oo.order_delivered_customer_date is not null group by pcnt.product_category_name_english order by Revenue asc limit 10""",
    "top_10_revenue_categories": """select pcnt.product_category_name_english Category, count(DISTINCT oo.order_id) Num_order, sum(oop.payment_value) Revenue from olist_orders oo join olist_order_items ooi on oo.order_id = ooi.order_id join olist_order_payments oop on oo.order_id = oop.order_id join olist_products op on ooi.product_id = op.product_id join product_category_name_translation pcnt on op.product_category_name = pcnt.product_category_name where oo.order_status = "delivered" and oo.order_delivered_customer_date is not null group by pcnt.product_category_name_english order by Revenue desc limit 10""",
    "real_vs_estimated_delivered_time": """with delivery_times as ( select julianday(oo.order_delivered_customer_date) - julianday(oo.order_purchase_timestamp) as real_time, julianday(oo.order_estimated_delivery_date) - julianday(oo.order_purchase_timestamp) as estimated_time, STRFTIME("%m", oo.order_purchase_timestamp) as month_no, case strftime("%m", oo.order_purchase_timestamp) WHEN '01' THEN 'Ene' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar' WHEN '04' THEN 'Abr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun' WHEN '07' THEN 'Jul' WHEN '08' THEN 'Ago' WHEN '09' THEN 'Sep' WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dic' end as month, strftime("%Y", oo.order_purchase_timestamp) as year_date from olist_orders oo where oo.order_status = "delivered" and oo.order_delivered_customer_date is not null ) select d.month_no, d.month, avg(case when d.year_date = "2016" then d.real_time end) as Year2016_real_time, avg(case when d.year_date = "2017" then d.real_time end) as Year2017_real_time, avg(case when d.year_date = "2018" then d.real_time end) as Year2018_real_time, avg(case when d.year_date = "2016" then d.estimated_time end) as Year2016_estimated_time, avg(case when d.year_date = "2017" then d.estimated_time end) as Year2017_estimated_time, avg(case when d.year_date = "2018" then d.estimated_time end) as Year2018_estimated_time from delivery_times d group by d.month_no, d.month having d.month_no is not null order by d.month_no""",
    "orders_per_day_and_holidays_2017": None,
    "get_freight_value_weight_relationship": None,
    # Puedes agregar más queries aquí
}

def orders_per_day_and_holidays_2017(database: Engine):
    query_name = "orders_per_day_and_holidays_2017"
    holidays = read_sql("SELECT * FROM public_holidays", database)
    orders = read_sql("SELECT * FROM olist_orders", database)
    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"]).dt.normalize()
    filtered_dates = orders[orders["order_purchase_timestamp"].dt.year == 2017]
    order_purchase_ammount_per_date = filtered_dates.groupby(filtered_dates["order_purchase_timestamp"]).size()
    result_df = order_purchase_ammount_per_date.reset_index()
    result_df.columns = ["date", "order_count"]
    result_df["holiday"] = result_df["date"].isin(holidays["date"])
    return result_df

def get_freight_value_weight_relationship(database: Engine):
    query_name = "get_freight_value_weight_relationship"
    orders = read_sql("SELECT * FROM olist_orders", database)
    items = read_sql("SELECT * FROM olist_order_items", database)
    products = read_sql("SELECT * FROM olist_products", database)
    data = pd.merge(items, orders, on="order_id", how="inner")
    data = pd.merge(data, products, on="product_id", how="inner")
    delivered = data[data["order_status"] == "delivered"]
    aggregations = delivered.groupby("order_id")[["freight_value", "product_weight_g"]].sum().reset_index()
    return aggregations

def ejecutar_queries():
    if not os.path.exists(DB_INPUT_PATH):
        print(f"❌ BD de origen no encontrada: {DB_INPUT_PATH}")
        return

    print("📥 Conectando a base de datos origen...")
    conn_in = create_engine(f"sqlite:///{DB_INPUT_PATH}")

    print("📤 Preparando base de datos de salida...")
    if os.path.exists(DB_OUTPUT_PATH):
        os.remove(DB_OUTPUT_PATH)
    conn_out = create_engine(f"sqlite:///{DB_OUTPUT_PATH}")

    for nombre_tabla, query in queries_dict.items():
        try:
            if query is None:
                print(f"🧠 Ejecutando función personalizada: {nombre_tabla}()")
                if nombre_tabla == "orders_per_day_and_holidays_2017":
                    df = orders_per_day_and_holidays_2017(conn_in)
                elif nombre_tabla == "get_freight_value_weight_relationship":
                    df = get_freight_value_weight_relationship(conn_in)
                else:
                    print(f"⚠️ Función '{nombre_tabla}' no encontrada. Saltando.")
            else:
                print(f"📊 Ejecutando query SQL: {nombre_tabla}")
                df = pd.read_sql(sql=query, con=conn_in)

            df.to_sql(nombre_tabla, con=conn_out, if_exists="replace", index=False)
            print(f"✅ Tabla '{nombre_tabla}' guardada con {len(df)} filas.")
        except Exception as e:
            print(f"❌ Error procesando '{nombre_tabla}': {e}")

    print("🏁 Finalizado procesamiento de resultados.")

with DAG(
    dag_id="apply_queries",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["transform", "sqlite", "resultados"]
) as dag:

    generar_resultados = PythonOperator(
        task_id="generar_resultados",
        python_callable=ejecutar_queries
    )
