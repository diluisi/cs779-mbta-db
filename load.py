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
    reader = csv.reader(open('stops.csv'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    next(reader)  # Skip header
    for l in reader:
        print(l[0])
        sql = "SELECT stop_id FROM stops WHERE stop_id='%s'" % (l[0])
        c.execute(sql)
        exists = c.fetchone()

        if not exists:
            sql = "SELECT municipality_id FROM municipalities WHERE municipality='%s'" % (l[6])
            c.execute(sql)
            print(l[6])
            municipality_id = c.fetchone()[0]

            sql = "SELECT street_id FROM streets WHERE street='%s'" % (l[6])
            c.execute(sql)
            at_street = c.fetchone()

            sql = "SELECT street_id FROM streets WHERE street='%s'" % (l[8])
            c.execute(sql)
            on_street = c.fetchone()

            if at_street and on_street:
                sql = """INSERT INTO STOPS (STOP_ID, ADDRESS, AT_STREET, DESCRIPTION, LATITUDE, LONGITUDE, MUNICIPALITY_ID, NAME, ON_STREET) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
                    l[0], l[1], at_street[0], l[3], l[4], l[5], municipality_id, l[7], on_street[0])
                c.execute(sql)
            if not at_street and on_street:
                sql = """INSERT INTO STOPS (STOP_ID, ADDRESS, DESCRIPTION, LATITUDE, LONGITUDE, MUNICIPALITY_ID, NAME, ON_STREET) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
                    l[0], l[1], l[3], l[4], l[5], municipality_id, l[7], on_street[0])
                print(sql)
                c.execute(sql)
            if at_street and not on_street:
                sql = """INSERT INTO STOPS (STOP_ID, ADDRESS, DESCRIPTION, LATITUDE, LONGITUDE, MUNICIPALITY_ID, NAME) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
                l[0], l[1], l[3], l[4], l[5], municipality_id, l[7])
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


if __name__ == '__main__':
    # load_directions_ids()
    # load_destinations()
    # load_colors()
    # load_lines()
    # load_routes_direction_names()
    # load_routes()

    # load_direction_names_routes_bridge()
    # load_destinations_routes_bridge()
    # load_municipalities()
    # load_streets()
    load_stops()

    c.execute('select * from colors')
    for row in c:
        print(row)
    conn.close()
