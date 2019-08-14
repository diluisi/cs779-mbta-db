# Load extracted structured data into the database
import cx_Oracle
import os

password = os.environ.get('ORACLE_PASSWORD')

dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orclpdb1')
conn = cx_Oracle.connect(user='system', password=password, dsn=dsn_tns)
c = conn.cursor()
c.execute('select * from DVD')
for row in c:
    print(row)
conn.close()
