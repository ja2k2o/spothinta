#!/usr/bin/python3

from datetime import datetime, timedelta
import configparser
import logging
import requests
from requests.exceptions import HTTPError


# Set this to logging.DEBUG in odrder to see debug messages
loglevel = logging.INFO


def get_spot_data(config):
    today = datetime.now(None)
    tomorrow = today + timedelta(days=1)
    day = tomorrow.strftime('%Y-%m-%d')

    url = config.get('url')
    payload = {'start': day+'T00:00:00.000Z', 'end': day+'T23:59:59.999Z'}

    logging.info(f'Retrieving data for {day} using {url}')

    try:
        response = requests.get(url, params=payload)
        response.raise_for_status()
        data = response.json()
    
    except HTTPError as http_err:
        logging.critical(f'An error occurred: {http_err}')
        logging.debug(f'Headers: {response.request.headers}')
        logging.debug(f'Body: {response.request.body}')
        exit(1)

    logging.info(f'Data retrieved')
    logging.debug(f"{data['data']['fi']}")
    return(data['data']['fi'])


def write_to_influxdb(config, data):
    url = config.get('url')
    usr = config.get('tunnus')
    pwd = config.get('salasana')

    params = {'db': 'electricity',
              'rp': 'autogen',
              'precision': 's',
              'consistency':'one'}
    
    logging.info(f'Preparing to send data to {url}')
    logging.debug(f'{data}')

    session = requests.Session()
    session.auth = (usr, pwd)

    for item in data:
        record = f'prices spot={item[1]},total={item[2]} {item[0]}'

        try:
            response = session.post(url, params=params, data=record)
            response.raise_for_status()

        except HTTPError as http_err:
            logging.critical(f'An error occured: {http_err}')
            logging.debug(f'Headers: {response.request.headers}')
            logging.debug(f'Body: {response.request.body}')
            exit(2)

    session.close()

    logging.info(f'Data sent')
    return


if __name__ == "__main__":
    logging.basicConfig(level=loglevel)

    config = configparser.ConfigParser()
    config.read('spothinta.ini')

    sahkovero = config.getfloat('hinnat', 'sahkovero')
    valityspalkkio = config.getfloat('hinnat', 'valityspalkkio')
    paivasiirto = config.getfloat('hinnat', 'paivasiirto')
    yosiirto = config.getfloat('hinnat', 'yosiirto')
    alv = config.getfloat('hinnat', 'alv')

    spot_data = get_spot_data(config['tietolahde'])

    data_points = []
    for data_point in spot_data:
        spot_hinta = float(data_point['price'] / 10) * (1 + alv / 100) # add ALV

        dt = datetime.fromtimestamp(data_point['timestamp'], None)
        if int(dt.strftime("%H")) in [22, 23, 0, 1, 2, 3, 4, 5, 6]:
            hinta = spot_hinta + sahkovero + valityspalkkio + yosiirto
        else:
            hinta = spot_hinta + sahkovero + valityspalkkio + paivasiirto

        data_points.append([data_point['timestamp'], spot_hinta, hinta])
        
    write_to_influxdb(config['tallennuspaikka'], data_points)
