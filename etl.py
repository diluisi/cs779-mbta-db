import requests as r

base_url = 'https://api-v3.mbta.com/'
routes_url = base_url + 'routes'
schedules_url = ''
stops_url = base_url + 'stops'
trips_url = ''
lines_url = base_url + 'lines'
route_patterns_url = ''
vehicles_url = base_url + 'vehicles'
shapes_url = ''
services_url = ''


def generate_vehicle_csv():
    vehicles = r.get(vehicles_url).json()

    for vehicle in vehicles['data']:
        with open('vehicles.csv', 'a') as fs:
            fs.write(vehicle['id'] + '\n')


def generate_routes_csv():
    routes = r.get(routes_url).json()

    for route in routes['data']:
        with open('routes.csv', 'a') as fs:
            fs.write(route['id'] + '\n')


def generate_stops_csv():
    stops = r.get(stops_url).json()

    for stop in stops['data']:
        with open('stops.csv', 'a') as fs:
            fs.write(stop['id'] + '\n')


def generate_lines_csv():
    lines = r.get(lines_url).json()

    for line in lines['data']:
        with open('lines.csv', 'a') as fs:
            fs.write(line['id'] + '\n')

def generate_schedules_csv():
    with open('routes.csv', 'r') as fs:
        pass


if __name__ == '__main__':
    generate_routes_csv()
    generate_lines_csv()
    generate_stops_csv()
    generate_vehicle_csv()
