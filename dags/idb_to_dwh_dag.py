from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.load_utils_dwh import load_dimension_or_fact

from transform.dim.transform_dim_time     import EXTRACT_SQL as SQL_TIME,     TARGET_COLUMNS as COL_TIME,     TARGET_TABLE as TBL_TIME
from transform.dim.transform_dim_location import EXTRACT_SQL as SQL_LOC,      TARGET_COLUMNS as COL_LOC,      TARGET_TABLE as TBL_LOC
from transform.dim.transform_dim_product  import EXTRACT_SQL as SQL_PROD,     TARGET_COLUMNS as COL_PROD,     TARGET_TABLE as TBL_PROD
from transform.dim.transform_dim_store    import EXTRACT_SQL as SQL_STORE,    TARGET_COLUMNS as COL_STORE,    TARGET_TABLE as TBL_STORE
from transform.dim.transform_dim_customer import EXTRACT_SQL as SQL_CUST,     TARGET_COLUMNS as COL_CUST,     TARGET_TABLE as TBL_CUST
from transform.fact.transform_fact_sales  import EXTRACT_SQL as SQL_SALES,    TARGET_COLUMNS as COL_SALES,    TARGET_TABLE as TBL_SALES
from transform.fact.transform_fact_inventory import EXTRACT_SQL as SQL_INV,   TARGET_COLUMNS as COL_INV,      TARGET_TABLE as TBL_INV

default_args = {'owner': 'data_team', 'start_date': datetime(2024, 1, 1)}

with DAG(
    dag_id='idb_to_dwh',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Load data từ IDB vào DWH (Star Schema)'
) as dag:

    # ── Tầng 1: Dim không phụ thuộc FK ───────────────────────
    t_dim_time = PythonOperator(
        task_id='load_dim_time',
        python_callable=load_dimension_or_fact,
        op_kwargs={
            'target_table': TBL_TIME, 'extract_sql': SQL_TIME,
            'target_columns': COL_TIME,
            'pk_conflict_action': '(time_key) DO NOTHING'
        }
    )

    t_dim_location = PythonOperator(
        task_id='load_dim_location',
        python_callable=load_dimension_or_fact,
        op_kwargs={
            'target_table': TBL_LOC, 'extract_sql': SQL_LOC,
            'target_columns': COL_LOC,
            'pk_conflict_action': '(location_key) DO UPDATE SET city=EXCLUDED.city'
        }
    )

    t_dim_product = PythonOperator(
        task_id='load_dim_product',
        python_callable=load_dimension_or_fact,
        op_kwargs={
            'target_table': TBL_PROD, 'extract_sql': SQL_PROD,
            'target_columns': COL_PROD,
            'pk_conflict_action': '(product_key) DO UPDATE SET description=EXCLUDED.description'
        }
    )

    # ── Tầng 2: Dim phụ thuộc Dim_Location ───────────────────
    t_dim_store = PythonOperator(
        task_id='load_dim_store',
        python_callable=load_dimension_or_fact,
        op_kwargs={
            'target_table': TBL_STORE, 'extract_sql': SQL_STORE,
            'target_columns': COL_STORE,
            'pk_conflict_action': '(store_key) DO NOTHING'
        }
    )

    t_dim_customer = PythonOperator(
        task_id='load_dim_customer',
        python_callable=load_dimension_or_fact,
        op_kwargs={
            'target_table': TBL_CUST, 'extract_sql': SQL_CUST,
            'target_columns': COL_CUST,
            'pk_conflict_action': '(customer_key) DO UPDATE SET customer_type=EXCLUDED.customer_type'
        }
    )

    # ── Tầng 3: Fact phụ thuộc tất cả Dim ────────────────────
    t_fact_sales = PythonOperator(
        task_id='load_fact_sales',
        python_callable=load_dimension_or_fact,
        op_kwargs={
            'target_table': TBL_SALES, 'extract_sql': SQL_SALES,
            'target_columns': COL_SALES,
            'pk_conflict_action': ('(time_key, product_key, customer_key) DO UPDATE SET '
                                   'quantity_ordered=Fact_Sales.quantity_ordered + EXCLUDED.quantity_ordered, '
                                   'total_amount=Fact_Sales.total_amount + EXCLUDED.total_amount')
        }
    )

    t_fact_inventory = PythonOperator(
        task_id='load_fact_inventory',
        python_callable=load_dimension_or_fact,
        op_kwargs={
            'target_table': TBL_INV, 'extract_sql': SQL_INV,
            'target_columns': COL_INV,
            'pk_conflict_action': '(time_key, store_key, product_key) DO UPDATE SET quantity_on_hand=EXCLUDED.quantity_on_hand'
        }
    )

    # ── Dependency graph ─────────────────────────────────────
    t_dim_location >> t_dim_customer
    t_dim_location >> t_dim_store
    [t_dim_time, t_dim_product, t_dim_customer] >> t_fact_sales
    [t_dim_time, t_dim_store, t_dim_product]    >> t_fact_inventory