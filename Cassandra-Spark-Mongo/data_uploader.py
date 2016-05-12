from cassandra.cluster import Cluster
import csv

file_name = 'data/transaction_data.csv'

cluster = Cluster()
session = cluster.connect('mykeyspace')
try:
    session.execute('DROP TABLE mykeyspace.transactions_test')
except:
    pass
session.execute('CREATE TABLE transactions_test (\
                transaction_id int PRIMARY KEY,\
                a_tenant_id text,\
                b_user_id int,\
                c_item_id int);')

with open(file_name, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        data = [int(row[0]),row[1],int(row[2]),int(row[3])]
        # use %s for all types of arguments
        session.execute(
            """
            INSERT INTO transactions_test(transaction_id,a_tenant_id,b_user_id,c_item_id)
            VALUES(%s,%s,%s,%s)
            """,
            data
        )
