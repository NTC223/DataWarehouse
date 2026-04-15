from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.dwh_to_olap import load_olap_cuboid
# Import gom nhóm lại cho gọn
import transform.olap as olap_config 

default_args = {'owner': 'data_team', 'start_date': datetime(2024, 1, 1)}

with DAG(
    dag_id='dwh_to_olap',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Load data từ DWH vào OLAP schema'
) as dag:

    cube1_suffixes = [
        'all', 'time', 'product', 'customer', 'location', 
        'time_product', 'time_customer', 'time_location',
        'product_customer', 'product_location', 'customer_location',
        'time_product_customer', 'time_product_location', 
        'time_customer_location', 'product_customer_location', 'base'
    ]

    for suffix in cube1_suffixes:
        from importlib import import_module
        mod = import_module(f"transform.olap.cube1_{suffix}")
        
        PythonOperator(
            task_id=f'load_cube1_{suffix}',
            python_callable=load_olap_cuboid,
            op_kwargs={
                'cuboid_table'  : mod.TARGET_TABLE,
                'extract_sql'   : mod.EXTRACT_SQL,
                'target_columns': mod.TARGET_COLUMNS,
            }
        )

    cube2_suffixes = [
        'all', 'time', 'product', 'store', 'location', 
        'time_product', 'time_store', 'time_location',
        'product_store', 'product_location', 'store_location', 
        'time_product_store', 'time_product_location', 
        'time_store_location', 'product_store_location', 'base'
    ]
    
    for suffix in cube2_suffixes:
        mod = import_module(f"transform.olap.cube2_{suffix}")
        PythonOperator(
            task_id=f'load_cube2_{suffix}',
            python_callable=load_olap_cuboid,
            op_kwargs={
                'cuboid_table'  : mod.TARGET_TABLE,
                'extract_sql'   : mod.EXTRACT_SQL,
                'target_columns': mod.TARGET_COLUMNS,
            }
        )