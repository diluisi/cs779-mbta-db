# Dump data to csv
import cx_Oracle
import os

password = os.environ.get('ORACLE_PASSWORD')

dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orclpdb1')
conn = cx_Oracle.connect(user='system', password=password, dsn=dsn_tns)

c = conn.cursor()
sql = """SELECT * FROM VEHICLES_DATA_HISTORY v 
        LEFT JOIN directions d ON d.direction_id=v.direction_id
        LEFT JOIN routes r ON r.route_id=v.route_id 
        LEFT JOIN statuses s ON s.status_id=v.current_status 
        LEFT JOIN stops st ON st.stop_id=v.stop_id
        LEFT JOIN municipalities m ON st.municipality_id=m.municipality_id
        LEFT JOIN streets str ON str.street_id=st.on_street 
        LEFT JOIN streets str ON str.street_id=st.at_street
        LEFT JOIN lines l ON l.line_id=r.line_id
        INNER JOIN colors c ON c.color_id=l.color 
        INNER JOIN colors c ON c.color_id=l.text_color 
        INNER JOIN colors c ON c.color_id=r.color 
        INNER JOIN colors c ON c.color_id=r.text_color
"""

c.execute(sql)

col_names = [row[0] for row in c.description]

rows = c.fetchall()

with open('dump.csv', 'w') as fs:
    fs.writelines(','.join(col_names))

with open('dump.csv', 'a') as fs:
    for row in rows:
        fs.write('\n')
        fs.writelines(','.join(['"%s"' % str(r) for r in row]))

conn.close()
