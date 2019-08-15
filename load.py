# Load extracted structured data into the database
import cx_Oracle
import os
import csv

password = os.environ.get('ORACLE_PASSWORD')

dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orclpdb1')
conn = cx_Oracle.connect(user='system', password=password, dsn=dsn_tns)
c = conn.cursor()


def load_statuses():
    reader = csv.reader(open('vehicles-current_statuses.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header
    for l in reader:
        sql = "INSERT INTO STATUSES (status) VALUES ('%s')" % l[0]
        c.execute(sql)

    conn.commit()


def load_direction_ids():
    reader = csv.reader(open('vehicles-direction_ids.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header
    for l in reader:
        sql = "INSERT INTO DIRECTIONS (direction) VALUES ('%s')" % l[0]
        c.execute(sql)

    conn.commit()


def load_vehicles():
    reader = csv.reader(open('vehicles.csv'))
    next(reader)  # Skip header
    for l in reader:
        sql = "INSERT INTO VEHICLES (vehicle_id, label) VALUES ('%s', '%s')" % (l[0], l[1])
        c.execute(sql)


def load_stops():
    reader = csv.reader(open('vehicles.csv'))
    next(reader)  # Skip header
    for l in reader:
        sql = "INSERT INTO VEHICLES (vehicle_id, label) VALUES ('%s', '%s')" % (l[0], l[1])
        c.execute(sql)


def load_routes_direction_names():
    pass


def load_direction_destinations():
    pass


def load_routes():
    pass


def load_lines():
    reader = csv.reader(open('lines.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[1])
        c.execute(sql)
        color = c.fetchone()[0]

        sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[4])
        c.execute(sql)
        text_color = c.fetchone()[0]

        sql = "INSERT INTO LINES (line_id, color, long_name, short_name, text_color) " \
              "VALUES ('%s', '%s', '%s', '%s', '%s')" % (l[0], color, l[2], l[3], text_color)
        print(sql)
        c.execute(sql)

    conn.commit()


def load_colors():
    reader = csv.reader(open('lines.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT color FROM colors WHERE color='%s'" % (l[1])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "INSERT INTO COLORS (color) VALUES ('%s')" % (l[1])
            c.execute(sql)

        sql = "SELECT color FROM colors WHERE color='%s'" % (l[4])
        c.execute(sql)
        exists = c.fetchone()
        if not exists:
            sql = "INSERT INTO COLORS (color) VALUES ('%s')" % (l[4])
            c.execute(sql)

    conn.commit()


if __name__ == '__main__':
    load_colors()
    load_lines()

    c.execute('select * from colors')
    for row in c:
        print(row)
    conn.close()
