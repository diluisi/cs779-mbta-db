# Fetch ids and store as csv files

import requests as r
import os
from mbta import *


def generate_vehicle_csv():
    headers = ['id', 'label', 'bearing', 'current_stop_sequence', 'direction_id', 'longitude', 'latitude', 'speed',
               'updated_at', 'current_status']
    rels = ['route_id', 'stop_id']
    direction_ids = []
    current_statuses = []

    vehicles = r.get(vehicles_url).json()

    with open('vehicles.csv', 'a') as fs:
        fs.write(','.join(headers + rels))

        for vehicle in vehicles['data']:
            fs.write('\n' + ','.join(
                ['"%s"' % vehicle[header] if header in vehicle else '"%s"' % vehicle['attributes'][header] for header in
                 headers]))

            try:
                route_id = vehicle['relationships']['route']['data']['id']
            except Exception:
                route_id = ''

            try:
                stop_id = vehicle['relationships']['stop']['data']['id']
            except Exception:
                stop_id = ''

            fs.write(',"%s"' % route_id)
            fs.write(',"%s"' % stop_id)

            current_statuses.append(vehicle['attributes']['current_status'])
            direction_ids.append(str(vehicle['attributes']['direction_id']))

    # Remove duplicates
    current_statuses = list(set(current_statuses))
    direction_ids = list(set(direction_ids))

    with open('vehicles-current_statuses.csv', 'a') as fs:
        fs.write('current_statuses')
        for current_status in current_statuses:
            fs.write('\n' + '"%s"' % current_status)

    with open('vehicles-direction_ids.csv', 'a') as fs:
        fs.write('direction_ids')
        for direction_id in direction_ids:
            fs.write('\n' + '"%s"' % direction_id)


def generate_routes_csv():
    headers = ['id', 'color', 'description', 'fare_class', 'long_name', 'short_name', 'text_color', 'direction_names',
               'direction_destinations']
    rels = ['line_id']
    direction_destinations = []
    direction_names = []

    routes = r.get(routes_url).json()

    with open('routes.csv', 'a') as fs:
        fs.write(','.join(headers + rels))

        for route in routes['data']:
            fs.write('\n' + ','.join(
                ['"%s"' % route[header] if header in route else '"%s"' % route['attributes'][header] for header in
                 headers]))

            try:
                line_id = route['relationships']['line']['data']['id']
            except Exception:
                line_id = ''

            fs.write(',"%s"' % line_id)

            for direction_destination in route['attributes']['direction_destinations']:
                direction_destinations.append(direction_destination)

            for direction_name in route['attributes']['direction_names']:
                direction_names.append(direction_name)

    # Remove duplicates
    direction_destinations = list(set(direction_destinations))
    direction_names = list(set(direction_names))

    with open('routes-direction_destinations.csv', 'a') as fs:
        fs.write('direction_destinations')
        for direction_destination in direction_destinations:
            fs.write('\n' + '"%s"' % direction_destination)

    with open('routes-direction_names.csv', 'a') as fs:
        fs.write('direction_names')
        for direction_name in direction_names:
            fs.write('\n' + '"%s"' % direction_name)


def generate_stops_csv():
    stops = r.get(stops_url).json()
    headers = ['id', 'address', 'at_street', 'description', 'latitude', 'longitude', 'municipality', 'name',
               'on_street']

    with open('stops.csv', 'a') as fs:
        fs.write(','.join(headers))

        for stop in stops['data']:
            fs.write('\n')
            line = [stop[header] if header in stop else stop['attributes'][header] for header in headers]
            line = ['"%s"' % str(l) if l is not None else '' for l in line]
            fs.write(','.join(line))


def generate_lines_csv():
    headers = ['id', 'color', 'long_name', 'short_name', 'text_color']

    lines = r.get(lines_url).json()

    with open('lines.csv', 'a') as fs:
        fs.write(','.join(headers))

        for line in lines['data']:
            fs.write('\n' + ','.join(
                ['"%s"' % line[header] if header in line else '"%s"' % line['attributes'][header] for header in
                 headers]))


if __name__ == '__main__':
    for item in os.listdir('.'):
        if item.endswith(".csv"):
            os.remove(os.path.join('.', item))

    generate_routes_csv()
    generate_lines_csv()
    generate_stops_csv()
    generate_vehicle_csv()
