'''
Process CA zip codes and convert to GPS coordinates
'''
import csv
from geopy.geocoders import Nominatim
from multiprocessing import Pool
import logging

ZIPCODE_CSV_FILE = '/home/rahul/dev/zipcodes_to_gps/CA_ZIPCODES_0.csv'
GEOLOCATOR = Nominatim()
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
        loc = GEOLOCATOR.geocode(postalcode_dict)
        logging.info(loc)
    except Exception as e:
        logging.exception(e)
        loc = None
    coordinate = {loc.latitude, loc.longitude} if loc else ""
    return coordinate

def main():
    zipcodes = get_zipcodes()
    get_coordinates(zipcodes)

if __name__ == '__main__':
    main()
