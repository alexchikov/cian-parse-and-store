import requests
import json
import logging
import yaml
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    filename='logs/parser.log',
                    format='[%(asctime)s] %(levelname)s: %(message)s')


class Parser(object):
    """
    Parser's Class
    """

    def __init__(self) -> None:
        with open('configs/config.yml') as config_file:  # uploading config
            config_file = yaml.safe_load(config_file)
            self.__cookies = config_file['cookies']
            self.__headers = config_file['headers']
            self.__json_data = config_file['json_data']

        logging.info('Initialized Parser object')

    def get_offers(self) -> (int, str):  # function for getting current offers
        try:
            response = requests.post(
                'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
                cookies=self.__cookies,
                headers=self.__headers,
                json=self.__json_data,
            )

            data = response.json()
            filename = f'cian_{datetime.strftime(datetime.now(), "%Y-%m-%d_%H:%M:%S")}.json'
            with open(f'files/{filename}', 'w') as file:
                json.dump(obj=data,
                          fp=file,
                          indent=4,
                          ensure_ascii=False)

            return (response.status_code, filename)
        except (requests.exceptions.ConnectionError) as exc:
            logging.error(f'{exc.__str__()}')
            return (400, 'Request Failed')

    def get_serialized_offers(self) -> dict:
        response_status_code, offers_filename = self.__get_offers()

        if response_status_code == 200:
            logging.info('Successfully get current offers')
        else:
            return (400, 'Request Failed')

        with open(f'files/{offers_filename}') as offers_file:
            offers = json.load(offers_file)
        serialized_offers = offers['data']['offersSerialized']

        return serialized_offers
    