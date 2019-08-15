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


def load_directions_ids():
    reader = csv.reader(open('vehicles-direction_ids.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header
    for l in reader:
        sql = "SELECT direction FROM directions WHERE direction='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
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
    reader = csv.reader(open('routes-direction_names.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT direction_name FROM direction_names WHERE direction_name='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "INSERT INTO direction_names (direction_name) VALUES ('%s')" % (l[0])
            c.execute(sql)
    conn.commit()


def load_destinations():
    reader = csv.reader(open('routes-direction_destinations.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header
    for l in reader:
        sql = "SELECT destination FROM destinations WHERE destination='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "INSERT INTO DESTINATIONS (destination) VALUES ('%s')" % (l[0])
            c.execute(sql)
    conn.commit()


def load_routes():
    reader = csv.reader(open('routes.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header
    for l in reader:
        sql = "SELECT route_id FROM routes WHERE route_id='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[1])
            c.execute(sql)
            color = c.fetchone()[0]

            sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[6])
            c.execute(sql)
            text_color = c.fetchone()[0]

            sql = "SELECT line_id FROM lines WHERE line_id='%s'" % (l[9])
            c.execute(sql)
            line_id = c.fetchone()

            if line_id:
                sql = "INSERT INTO routes (route_id, color, description, fare_class, long_name, short_name, text_color, line_id) " \
                      "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                          l[0], color, l[2], l[3], l[4], l[5], text_color, line_id[0])
                c.execute(sql)
            else:
                sql = "INSERT INTO routes (route_id, color, description, fare_class, long_name, short_name, text_color) " \
                      "VALUES ('%s','%s','%s','%s','%s','%s','%s')" % (
                          l[0], color, l[2], l[3], l[4], l[5], text_color)
                c.execute(sql)

    conn.commit()


def load_lines():
    reader = csv.reader(open('lines.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT line_id FROM lines WHERE line_id='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[1])
            c.execute(sql)
            color = c.fetchone()[0]

            sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[4])
            c.execute(sql)
            text_color = c.fetchone()[0]

            sql = "INSERT INTO LINES (line_id, color, long_name, short_name, text_color) " \
                  "VALUES ('%s', '%s', '%s', '%s', '%s')" % (l[0], color, l[2], l[3], text_color)
            c.execute(sql)

    reader = csv.reader(open('routes.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT line_id FROM lines WHERE line_id='%s'" % (l[9])
        c.execute(sql)
        exists = c.fetchone()

        if not exists and l[9] != '':
            sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[1])
            c.execute(sql)
            color = c.fetchone()[0]

            sql = "SELECT color_id FROM colors WHERE color='%s'" % (l[6])
            c.execute(sql)
            text_color = c.fetchone()[0]

            sql = "INSERT INTO LINES (line_id, color, long_name, short_name, text_color) " \
                  "VALUES ('%s', '%s', '%s', '%s', '%s')" % (l[9], color, l[4], l[5], text_color)
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

    reader = csv.reader(open('routes.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT color FROM colors WHERE color='%s'" % (l[1])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "INSERT INTO COLORS (color) VALUES ('%s')" % (l[1])
            c.execute(sql)

        sql = "SELECT color FROM colors WHERE color='%s'" % (l[6])
        c.execute(sql)
        exists = c.fetchone()
        if not exists:
            sql = "INSERT INTO COLORS (color) VALUES ('%s')" % (l[6])
            c.execute(sql)

    conn.commit()


if __name__ == '__main__':
    load_directions_ids()
    load_destinations()
    load_colors()
    load_lines()
    load_routes_direction_names()
    load_routes()

    c.execute('select * from colors')
    for row in c:
        print(row)
    conn.close()
