from load_utils_idb import load_table
def extract_mail_order():
    load_table('mysql_default', 'MailOrderCustomer', "SELECT customer_id, postal_address, time FROM MailOrderCustomer WHERE time >= '{last_runtime}'", ['customer_id', 'postal_address', 'join_time'], '(customer_id) DO NOTHING')
