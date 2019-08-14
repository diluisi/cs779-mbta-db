# Load extracted structured data into the database
import cx_Oracle
import os
import csv

password = os.environ.get('ORACLE_PASSWORD')

dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orclpdb1')
conn = cx_Oracle.connect(user='system', password=password, dsn=dsn_tns)
c = conn.cursor()


def load_statuses():
    lines = []
    reader = csv.reader(open('vehicles-current_statuses.csv'))
    next(reader)  # Skip header
    for l in reader:
        sql = "INSERT INTO STATUSES (status) VALUES ('%s')" % l[0]
        c.execute(sql)

    conn.commit()


def load_direction_ids():
    pass


def load_vehicles():
    pass


def load_stops():
    pass


def load_routes_direction_names():
    pass


def load_direction_destinations():
    pass


def load_routes():
    pass


def load_lines():
    pass


if __name__ == '__main__':
    load_statuses()
    c.execute('select * from statuses')
    for row in c:
        print(row)
    conn.close()
