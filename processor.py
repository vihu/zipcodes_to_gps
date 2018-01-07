'''
Process CA zip codes and convert to GPS coordinates
'''
import csv
import logging
from multiprocessing import Pool
from geopy.geocoders import GeocodeFarm, GoogleV3, OpenCage
from config import ZIPCODE_CSV_FILE, OPENCAGE_API_KEY
from random import choice

GEOLOCATORS = [GoogleV3(), GeocodeFarm(), OpenCage(api_key=OPENCAGE_API_KEY)]
logging.basicConfig(filename='processor.log', level=logging.DEBUG)

def get_zipcodes(fname=ZIPCODE_CSV_FILE):
    '''
    return a list of zipcodes after reading the csv file.
    example: [{'postalcode': ####}, {'postalcode': ####}, ...]
    '''
    zipcodes = []
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            zipcodes.append({'postalcode': row[0]})
    return zipcodes

def get_coordinates(list_of_zipcodes):
    '''
    return a list of coordinates after reading list of zipcodes.
    '''
    pool = Pool(8)
    with open('coordinates.txt', 'w') as f:
        for coordinate in pool.imap_unordered(get_coordinate, list_of_zipcodes):
            f.write(str(coordinate) + '\n')

def get_coordinate(postalcode_dict):
    '''
    return Coordinate({lat, long}) from postalcode_dict being {'postalcode': ###}.
    '''
    try:
        locator = choice(GEOLOCATORS)
        loc = locator.geocode(postalcode_dict)
    except Exception as e:
        logging.exception(e)
        loc = None
    coordinate = {loc.latitude, loc.longitude} if loc else ""
    logging.info(coordinate)
    return coordinate

def main():
    zipcodes = get_zipcodes()
    get_coordinates(zipcodes)

if __name__ == '__main__':
    # pass
    main()
