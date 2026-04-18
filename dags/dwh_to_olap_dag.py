from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.dwh_to_olap import load_olap_cuboid
from transform.olap.olap_cuboid_defs import CUBE1, CUBE2

default_args = {'owner': 'data_team', 'start_date': datetime(2024, 1, 1)}

with DAG(
    dag_id='dwh_to_olap',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Load data từ DWH vào OLAP schema (full granularity)',
) as dag:

    def _make_tasks(cuboids: list, prefix: str) -> dict:
        """Tạo PythonOperator cho từng cuboid, trả về dict {table_name: task}."""
        tasks = {}
        for c in cuboids:
            tasks[c['table']] = PythonOperator(
                task_id=f"{prefix}__{c['table']}",
                python_callable=load_olap_cuboid,
                op_kwargs={
                    'cuboid_table'  : c['table'],
                    'extract_sql'   : c['sql'],
                    'target_columns': c['columns'],
                },
            )
        return tasks

    c1 = _make_tasks(CUBE1, 'c1')
    c2 = _make_tasks(CUBE2, 'c2')

    # ── Cube 1 dependencies ───────────────────────────────────
    # dim-0
    c1_all = c1['olap_sales_all']

    # dim-1
    c1_time     = c1['olap_sales_by_time']
    c1_quarter  = c1['olap_sales_by_quarter']
    c1_year     = c1['olap_sales_by_year']
    c1_cust_info = c1['olap_sales_by_customer_info']
    c1_cust_loc  = c1['olap_sales_by_customer_loc']
    c1_ctype    = c1['olap_sales_by_customer_type']
    c1_city     = c1['olap_sales_by_city']
    c1_state    = c1['olap_sales_by_state']
    c1_prod     = c1['olap_sales_by_product']

    # dim-1 → dim-2
    c1_all >> [c1_time, c1_quarter, c1_year,
               c1_cust_info, c1_cust_loc, c1_ctype, c1_city, c1_state,
               c1_prod]

    # month time × product/customer
    c1_time >> [c1['olap_sales_time_product'],
                c1['olap_sales_time_customer_info'],
                c1['olap_sales_time_customer_loc'],
                c1['olap_sales_month_customer_type'],
                c1['olap_sales_month_city'],
                c1['olap_sales_month_state']]
    c1_prod >> [c1['olap_sales_time_product'],
                c1['olap_sales_customer_product_info'],
                c1['olap_sales_customer_product_loc'],
                c1['olap_sales_product_customer_type'],
                c1['olap_sales_product_city'],
                c1['olap_sales_product_state']]
    c1_cust_info >> [c1['olap_sales_time_customer_info'],
                     c1['olap_sales_customer_product_info']]
    c1_cust_loc  >> [c1['olap_sales_time_customer_loc'],
                     c1['olap_sales_customer_product_loc']]

    # quarter time ×
    c1_quarter >> [c1['olap_sales_quarter_product'],
                   c1['olap_sales_quarter_customer_info'],
                   c1['olap_sales_quarter_customer_loc'],
                   c1['olap_sales_quarter_customer_type'],
                   c1['olap_sales_quarter_city'],
                   c1['olap_sales_quarter_state']]

    # year time ×
    c1_year >> [c1['olap_sales_year_product'],
                c1['olap_sales_year_customer_info'],
                c1['olap_sales_year_customer_loc'],
                c1['olap_sales_year_customer_type'],
                c1['olap_sales_year_city'],
                c1['olap_sales_year_state']]

    # dim-2 → dim-3 (base)
    c1_base_deps_info = [
        c1['olap_sales_time_product'],
        c1['olap_sales_time_customer_info'],
        c1['olap_sales_customer_product_info'],
    ]
    for t in c1_base_deps_info:
        t >> c1['olap_sales_base_info']

    c1_base_deps_loc = [
        c1['olap_sales_time_product'],
        c1['olap_sales_time_customer_loc'],
        c1['olap_sales_customer_product_loc'],
    ]
    for t in c1_base_deps_loc:
        t >> c1['olap_sales_base_loc']

    # month × product × customer variants
    c1['olap_sales_time_product']   >> [c1['olap_sales_month_product_customer_type'],
                                        c1['olap_sales_month_product_city'],
                                        c1['olap_sales_month_product_state']]
    c1['olap_sales_quarter_product'] >> [c1['olap_sales_quarter_product_customer_info'],
                                         c1['olap_sales_quarter_product_customer_loc'],
                                         c1['olap_sales_quarter_product_customer_type'],
                                         c1['olap_sales_quarter_product_city'],
                                         c1['olap_sales_quarter_product_state']]
    c1['olap_sales_year_product']    >> [c1['olap_sales_year_product_customer_info'],
                                         c1['olap_sales_year_product_customer_loc'],
                                         c1['olap_sales_year_product_customer_type'],
                                         c1['olap_sales_year_product_city'],
                                         c1['olap_sales_year_product_state']]

    # ── Cube 2 dependencies ───────────────────────────────────
    c2_all     = c2['olap_inv_all']
    c2_time    = c2['olap_inv_by_time']
    c2_quarter = c2['olap_inv_by_quarter']
    c2_year    = c2['olap_inv_by_year']
    c2_prod    = c2['olap_inv_by_product']
    c2_store   = c2['olap_inv_by_store']
    c2_city    = c2['olap_inv_by_city']
    c2_state   = c2['olap_inv_by_state']

    c2_all >> [c2_time, c2_quarter, c2_year,
               c2_prod,
               c2_store, c2_city, c2_state]

    # month time ×
    c2_time >> [c2['olap_inv_time_product'],
                c2['olap_inv_time_store'],
                c2['olap_inv_month_city'],
                c2['olap_inv_month_state']]
    c2_prod >> [c2['olap_inv_time_product'],
                c2['olap_inv_product_store'],
                c2['olap_inv_product_city'],
                c2['olap_inv_product_state']]
    c2_store >> [c2['olap_inv_time_store'],
                 c2['olap_inv_product_store']]

    # quarter time ×
    c2_quarter >> [c2['olap_inv_quarter_product'],
                   c2['olap_inv_quarter_store'],
                   c2['olap_inv_quarter_city'],
                   c2['olap_inv_quarter_state']]

    # year time ×
    c2_year >> [c2['olap_inv_year_product'],
                c2['olap_inv_year_store'],
                c2['olap_inv_year_city'],
                c2['olap_inv_year_state']]

    # dim-2 → base
    for t in [c2['olap_inv_time_product'],
              c2['olap_inv_time_store'],
              c2['olap_inv_product_store']]:
        t >> c2['olap_inv_base']

    # month product × store variants
    c2['olap_inv_time_product']    >> [c2['olap_inv_month_product_city'],
                                       c2['olap_inv_month_product_state']]
    c2['olap_inv_quarter_product'] >> [c2['olap_inv_quarter_product_store'],
                                       c2['olap_inv_quarter_product_city'],
                                       c2['olap_inv_quarter_product_state']]
    c2['olap_inv_year_product']    >> [c2['olap_inv_year_product_store'],
                                       c2['olap_inv_year_product_city'],
                                       c2['olap_inv_year_product_state']]