# Load extracted structured data into the database
import cx_Oracle
import os
import csv
import datetime

from dateutil import parser

password = os.environ.get('ORACLE_PASSWORD')

dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orclpdb1')
conn = cx_Oracle.connect(user='system', password=password, dsn=dsn_tns)
c = conn.cursor()


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


def load_stops():
    reader = csv.reader(open('stops.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header
    for l in reader:
        sql = "SELECT stop_id FROM stops WHERE stop_id='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()
        print(l)
        if not exists:
            sql = "SELECT municipality_id FROM municipalities WHERE municipality='%s'" % (l[6])
            c.execute(sql)
            municipality_id = c.fetchone()[0]

            sql = "SELECT street_id FROM streets WHERE street='%s'" % (l[2].replace("'", "''"))
            c.execute(sql)
            at_street = c.fetchone()

            sql = "SELECT street_id FROM streets WHERE street='%s'" % (l[8].replace("'", "''"))
            c.execute(sql)
            on_street = c.fetchone()

            if not at_street and on_street:
                sql = """INSERT INTO STOPS (STOP_ID, ADDRESS, DESCRIPTION, LATITUDE, LONGITUDE, MUNICIPALITY_ID, NAME, ON_STREET) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
                    l[0], l[1].replace("'", "''"), l[3].replace("'", "''"), l[4], l[5], municipality_id,
                    l[7].replace("'", "''"), on_street[0])
                c.execute(sql)
            elif not on_street and at_street:
                sql = """INSERT INTO STOPS (STOP_ID, ADDRESS, AT_STREET, DESCRIPTION, LATITUDE, LONGITUDE, MUNICIPALITY_ID, NAME) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
                    l[0], l[1].replace("'", "''"), at_street, l[3].replace("'", "''"), l[4], l[5], municipality_id,
                    l[7].replace("'", "''"))
                c.execute(sql)
            else:
                sql = """INSERT INTO STOPS (STOP_ID, ADDRESS, DESCRIPTION, LATITUDE, LONGITUDE, MUNICIPALITY_ID, NAME) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
                    l[0], l[1].replace("'", "''"), l[3].replace("'", "''"), l[4], l[5], municipality_id,
                    l[7].replace("'", "''"))
                c.execute(sql)

    conn.commit()


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


def load_direction_names_routes_bridge():
    sql = "SELECT direction_name_id FROM direction_names"
    c.execute(sql)
    direction_name_ids = c.fetchall()

    sql = "SELECT route_id FROM routes"
    c.execute(sql)
    route_ids = c.fetchall()

    for route_id in route_ids:
        for direction_name_id in direction_name_ids:
            sql = "SELECT route_id, direction_name_id FROM direction_names_routes_bridge " \
                  "WHERE route_id='%s' AND direction_name_id='%s'" % (route_id[0], direction_name_id[0])
            c.execute(sql)
            exists = c.fetchone()

            if not exists:
                sql = "INSERT INTO direction_names_routes_bridge (route_id, direction_name_id) " \
                      "VALUES ('%s', '%s')" % (route_id[0], direction_name_id[0])
                c.execute(sql)

    conn.commit()


def load_destinations_routes_bridge():
    sql = "SELECT destination_id FROM destinations"
    c.execute(sql)
    destination_ids = c.fetchall()

    sql = "SELECT route_id FROM routes"
    c.execute(sql)
    route_ids = c.fetchall()

    for route_id in route_ids:
        for destination_id in destination_ids:
            sql = "SELECT route_id, destination_id FROM destinations_routes_bridge " \
                  "WHERE route_id='%s' AND destination_id='%s'" % (route_id[0], destination_id[0])
            c.execute(sql)
            exists = c.fetchone()

            if not exists:
                sql = "INSERT INTO destinations_routes_bridge (route_id, destination_id) " \
                      "VALUES ('%s', '%s')" % (route_id[0], destination_id[0])
                c.execute(sql)

    conn.commit()


def load_municipalities():
    reader = csv.reader(open('stops.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT municipality_id FROM municipalities WHERE municipality='%s'" % (l[6])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "INSERT INTO MUNICIPALITIES (municipality) VALUES ('%s')" % l[6]
            c.execute(sql)
    conn.commit()


def load_streets():
    reader = csv.reader(open('stops.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        # At Street
        street = l[2].replace("'", "''"),
        sql = "SELECT street FROM streets WHERE street='%s'" % street
        c.execute(sql)
        exists = c.fetchone()

        if not exists and l[2] != '':
            sql = "INSERT INTO streets (street) VALUES ('%s')" % street
            c.execute(sql)

        # On Street
        street = l[8].replace("'", "''"),
        sql = "SELECT street FROM streets WHERE street='%s'" % street
        c.execute(sql)
        exists = c.fetchone()

        if not exists and l[8] != '':
            sql = "INSERT INTO streets (street) VALUES ('%s')" % street
            c.execute(sql)
    conn.commit()


def load_statuses():
    reader = csv.reader(open('vehicles-current_statuses.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        sql = "SELECT status FROM statuses WHERE status='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "INSERT INTO statuses (status) VALUES ('%s')" % l[0]
            c.execute(sql)
    conn.commit()


def load_vehicles_data():
    reader = csv.reader(open('vehicles.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header

    for l in reader:
        utc_delta = datetime.datetime.utcnow() - datetime.datetime.now()
        updated_at = parser.parse(l[8]) + utc_delta
        updated_at = updated_at.strftime('%Y-%m-%d %H:%M:%S')

        sql = "SELECT vehicle_id, updated_at FROM vehicles_data WHERE vehicle_id='%s' and updated_at=TO_DATE('%s','yyyy-mm-DD HH:MI:SS')" % (
            l[0], updated_at)
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "SELECT direction_id FROM directions WHERE direction='%s'" % (l[4])
            c.execute(sql)
            direction_id = c.fetchone()[0]

            try:
                sql = "SELECT route_id FROM routes WHERE route_id='%s'" % (l[10])
                c.execute(sql)
                route_id = c.fetchone()[0]
            except TypeError:
                print 'ROUTE_ID "%s" does not exist in ROUTES table - ignoring' % l[10]
                continue

            sql = "SELECT stop_id FROM stops WHERE stop_id='%s'" % (l[11])
            c.execute(sql)
            stop_id = c.fetchone()

            sql = "SELECT status_id FROM statuses WHERE status='%s'" % (l[9])
            c.execute(sql)
            status_id = c.fetchone()[0]

            speed = l[7]
            if speed == 'None' or not speed:
                speed = ''

            current_stop_sequence = l[3]
            if current_stop_sequence == 'None' or not current_stop_sequence:
                current_stop_sequence = ''

            bearing = l[2]
            if bearing == 'None' or not bearing:
                bearing = ''

            if not stop_id:
                sql = "INSERT INTO vehicles_data (vehicle_id, label, bearing, current_stop_sequence, longitude, latitude, speed, updated_at, direction_id, route_id, current_status) " \
                      "VALUES ('%s','%s','%s','%s','%s','%s','%s',TO_DATE('%s','yyyy-mm-DD HH:MI:SS'),'%s','%s','%s')" % (
                          l[0], l[1], bearing, current_stop_sequence, l[5], l[6], speed, updated_at, direction_id,
                          route_id, status_id)
                c.execute(sql)
            else:
                sql = "INSERT INTO vehicles_data (vehicle_id, label, bearing, current_stop_sequence, longitude, latitude, speed, updated_at, direction_id, route_id, current_status, stop_id) " \
                      "VALUES ('%s','%s','%s','%s','%s','%s','%s', TO_DATE('%s','yyyy-mm-DD HH:MI:SS'),'%s','%s','%s','%s')" % (
                          l[0], l[1], bearing, current_stop_sequence, l[5], l[6], speed, updated_at, direction_id,
                          route_id, status_id, stop_id[0])
                c.execute(sql)
        conn.commit()


if __name__ == '__main__':
    load_directions_ids()
    load_destinations()
    load_colors()
    load_lines()
    load_routes_direction_names()
    load_routes()
    load_direction_names_routes_bridge()
    load_destinations_routes_bridge()
    load_municipalities()
    load_streets()
    load_stops()
    load_statuses()
    load_vehicles_data()
