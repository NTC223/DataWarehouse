from load_utils_dwh import load_dimension_or_fact
def transform_dim_product():
    load_dimension_or_fact('Dim_Product', """SELECT product_id, description, size, weight, price FROM idb.Product WHERE created_time >= '{last_runtime}'""", ['product_id', 'description', 'size', 'weight', 'price'], False)
