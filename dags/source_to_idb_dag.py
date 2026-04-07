from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.load_utils_idb import load_table

from extract.from_postgres.extract_representative_office import (
    EXTRACT_SQL as SQL_RO, TARGET_TABLE as TBL_RO, TARGET_COLUMNS as COL_RO, SOURCE_CONN_ID as SRC_RO
)
from extract.from_mysql.extract_customer import (
    EXTRACT_SQL as SQL_CUST, TARGET_COLUMNS as COL_CUST, TARGET_TABLE as TBL_CUST, SOURCE_CONN_ID as SRC_CUST
)
from extract.from_postgres.extract_store import (
    EXTRACT_SQL as SQL_STORE, TARGET_COLUMNS as COL_STORE, TARGET_TABLE as TBL_STORE, SOURCE_CONN_ID as SRC_STORE
)
from extract.from_postgres.extract_product import (
    EXTRACT_SQL as SQL_PROD, TARGET_COLUMNS as COL_PROD, TARGET_TABLE as TBL_PROD, SOURCE_CONN_ID as SRC_PROD
)
from extract.from_mysql.extract_tourist import (
    EXTRACT_SQL as SQL_TOUR, TARGET_COLUMNS as COL_TOUR, TARGET_TABLE as TBL_TOUR, SOURCE_CONN_ID as SRC_TOUR
)
from extract.from_mysql.extract_mail_order import (
    EXTRACT_SQL as SQL_MAIL, TARGET_COLUMNS as COL_MAIL, TARGET_TABLE as TBL_MAIL, SOURCE_CONN_ID as SRC_MAIL
)
from extract.from_postgres.extract_order import (
    EXTRACT_SQL as SQL_ORD, TARGET_COLUMNS as COL_ORD, TARGET_TABLE as TBL_ORD, SOURCE_CONN_ID as SRC_ORD
)
from extract.from_postgres.extract_order_product import (
    EXTRACT_SQL as SQL_OP, TARGET_COLUMNS as COL_OP, TARGET_TABLE as TBL_OP, SOURCE_CONN_ID as SRC_OP
)
from extract.from_postgres.extract_stocked_product import (
    EXTRACT_SQL as SQL_SP, TARGET_COLUMNS as COL_SP, TARGET_TABLE as TBL_SP, SOURCE_CONN_ID as SRC_SP
)

default_args = {'owner': 'data_team', 'start_date': datetime(2024, 1, 1)}

with DAG(
    dag_id='source_to_idb',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Load data từ MySQL + PostgreSQL vào IDB'
) as dag:
    
    # ── Tầng 1: Không phụ thuộc FK ────────────────────────────
    t_rep_office = PythonOperator(
        task_id='load_representative_office',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_RO, 'target_table': TBL_RO,
            'extract_sql': SQL_RO, 'target_columns': COL_RO,
            'pk_conflict_action': '(city_id) DO UPDATE SET city_name=EXCLUDED.city_name, last_updated_time=EXCLUDED.last_updated_time'
        }
    )

    t_product = PythonOperator(
        task_id='load_product',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_PROD, 'target_table': TBL_PROD,
            'extract_sql': SQL_PROD, 'target_columns': COL_PROD,
            'pk_conflict_action': '(product_id) DO UPDATE SET price=EXCLUDED.price, last_updated_time=EXCLUDED.last_updated_time'
        }
    )
    # ── Tầng 2: Phụ thuộc RepresentativeOffice ───────────────
    t_customer = PythonOperator(
        task_id='load_customer',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_CUST, 'target_table': TBL_CUST,
            'extract_sql': SQL_CUST, 'target_columns': COL_CUST,
            'pk_conflict_action': '(customer_id) DO NOTHING'
        }
    )

    t_store = PythonOperator(
        task_id='load_store',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_STORE, 'target_table': TBL_STORE,
            'extract_sql': SQL_STORE, 'target_columns': COL_STORE,
            'pk_conflict_action': '(store_id) DO NOTHING'
        }
    )

    # ── Tầng 3: Phụ thuộc Customer ───────────────────────────
    t_tourist = PythonOperator(
        task_id='load_tourist_customer',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_TOUR, 'target_table': TBL_TOUR,
            'extract_sql': SQL_TOUR, 'target_columns': COL_TOUR,
            'pk_conflict_action': '(customer_id) DO NOTHING'
        }
    )

    t_mail = PythonOperator(
        task_id='load_mail_order_customer',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_MAIL, 'target_table': TBL_MAIL,
            'extract_sql': SQL_MAIL, 'target_columns': COL_MAIL,
            'pk_conflict_action': '(customer_id) DO NOTHING'
        }
    )

    t_order = PythonOperator(
        task_id='load_order',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_ORD, 'target_table': TBL_ORD,
            'extract_sql': SQL_ORD, 'target_columns': COL_ORD,
            'pk_conflict_action': '(order_id) DO NOTHING'
        }
    )

    # ── Tầng 4: Phụ thuộc Order + Product + Store ────────────
    t_order_product = PythonOperator(
        task_id='load_order_product',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_OP, 'target_table': TBL_OP,
            'extract_sql': SQL_OP, 'target_columns': COL_OP,
            'pk_conflict_action': '(order_id, product_id) DO NOTHING'
        }
    )

    t_stocked = PythonOperator(
        task_id='load_stocked_product',
        python_callable=load_table,
        op_kwargs={
            'source_conn_id': SRC_SP, 'target_table': TBL_SP,
            'extract_sql': SQL_SP, 'target_columns': COL_SP,
            'pk_conflict_action': '(store_id, product_id) DO UPDATE SET stock_quantity=EXCLUDED.stock_quantity, last_updated_time=EXCLUDED.last_updated_time'
        }
    )

    # ── Dependency graph ─────────────────────────────────────
    t_rep_office >> [t_customer, t_store]
    t_product    >> [t_order_product, t_stocked]
    t_customer   >> [t_tourist, t_mail, t_order]
    t_order      >> t_order_product
    t_store      >> t_stocked