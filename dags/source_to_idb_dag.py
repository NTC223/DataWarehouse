from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from extract_customer import extract_customer
from extract_tourist import extract_tourist
from extract_mail_order import extract_mail_order
from extract_representative_office import extract_representative_office
from extract_store import extract_store
from extract_product import extract_product
from extract_order import extract_order
from extract_stocked_product import extract_stocked_product
from extract_order_product import extract_order_product

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
    '01_source_to_idb_etl',
    default_args=default_args,
    description='Load data from Source DBs to IDB (Modular)',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['dwh', 'idb'],
) as dag:
    
    with TaskGroup("load_master_data") as master_group:
        t_office = PythonOperator(task_id='extract_representative_office', python_callable=extract_representative_office)
        t_product = PythonOperator(task_id='extract_product', python_callable=extract_product)
        t_customer = PythonOperator(task_id='extract_customer', python_callable=extract_customer)
        
    with TaskGroup("load_reference_data") as ref_group:
        t_store = PythonOperator(task_id='extract_store', python_callable=extract_store)
        t_tourist = PythonOperator(task_id='extract_tourist', python_callable=extract_tourist)
        t_mail = PythonOperator(task_id='extract_mail_order', python_callable=extract_mail_order)
        
    with TaskGroup("load_transactions") as trans_group:
        t_order = PythonOperator(task_id='extract_order', python_callable=extract_order)
        
    with TaskGroup("load_junctions") as junc_group:
        t_stocked = PythonOperator(task_id='extract_stocked_product', python_callable=extract_stocked_product)
        t_order_product = PythonOperator(task_id='extract_order_product', python_callable=extract_order_product)

    # Dependencies
    t_office >> [t_customer, t_store]
    t_customer >> [t_tourist, t_mail, t_order]
    [t_store, t_product] >> t_stocked
    [t_order, t_product] >> t_order_product
