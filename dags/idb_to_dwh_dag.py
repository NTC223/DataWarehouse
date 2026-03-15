from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from transform_dim_date import transform_dim_date
from transform_dim_location import transform_dim_location
from transform_dim_store import transform_dim_store
from transform_dim_product import transform_dim_product
from transform_dim_customer import transform_dim_customer
from transform_fact_sales import transform_fact_sales
from transform_fact_inventory import transform_fact_inventory

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '02_idb_to_dwh_etl',
    default_args=default_args,
    description='Load transformed data from IDB to Data Warehouse Star Schema',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['dwh', 'star_schema'],
) as dag:

    # Task đọc lập: Sinh Dim_Date (Calendar) - chạy trước tất cả
    task_dim_date = PythonOperator(task_id='transform_dim_date', python_callable=transform_dim_date)

    # Task Group Transform (Dimensions)
    with TaskGroup("transform_dimensions") as transform_group:
        task_dim_location = PythonOperator(task_id='transform_dim_location', python_callable=transform_dim_location)
        task_dim_store = PythonOperator(task_id='transform_dim_store', python_callable=transform_dim_store)
        task_dim_product = PythonOperator(task_id='transform_dim_product', python_callable=transform_dim_product)
        task_dim_customer = PythonOperator(task_id='transform_dim_customer', python_callable=transform_dim_customer)

    # Task Group Load (Facts)
    with TaskGroup("load_facts") as load_group:
        task_fact_sales = PythonOperator(task_id='transform_fact_sales', python_callable=transform_fact_sales)
        task_fact_inventory = PythonOperator(task_id='transform_fact_inventory', python_callable=transform_fact_inventory)

    # === DEPENDENCIES ===
    # Dim_Date chạy trước toàn bộ (Fact cần date_key FK từ Dim_Date)
    task_dim_date >> transform_group

    # Trong Dimensions: Location phải hoàn thành trước Store và Customer (vì FK city_key)
    task_dim_location >> [task_dim_store, task_dim_customer]

    # Toàn bộ Dimensions phải xong trước khi load Facts
    transform_group >> load_group

    # Fine-grained arrows (visible in Airflow Graph View)
    [task_dim_store, task_dim_customer, task_dim_product] >> task_fact_sales
    [task_dim_store, task_dim_product] >> task_fact_inventory
