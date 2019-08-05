import requests as r
from pprint import pprint

base_url = 'https://api-v3.mbta.com/'
routes_url = ''
schedules_url = ''
stops_url = ''
trips_url = ''
lines_url = ''
route_patterns_url = ''
vehicles_url = base_url + 'vehicles'
shapes_url = ''
services_url = ''

def generate_vehicle_csv():
    vehicles = r.get(vehicles_url).json()
    with open('vehicles.csv', 'w') as fs:
        fs.write('id')

    for vehicle in vehicles['data']:
        with open('vehicles.csv', 'a') as fs:
            fs.write(vehicle['id'])

def generate_routes_csv():
    vehicles = r.get(routes_url).json()
    with open('routes.csv', 'w') as fs:
        fs.write('id')

    for vehicle in vehicles['data']:
        with open('vehicles.csv', 'a') as fs:
            fs.write(vehicle['id'])

if __name__ == '__main__':
