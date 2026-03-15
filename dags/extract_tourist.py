from load_utils_idb import load_table
def extract_tourist():
    load_table('mysql_default', 'TouristCustomer', "SELECT customer_id, tour_guide, time FROM TouristCustomer WHERE time >= '{last_runtime}'", ['customer_id', 'tour_guide', 'tour_time'], '(customer_id) DO NOTHING')
