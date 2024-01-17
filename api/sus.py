import os
import requests
from requests.auth import HTTPBasicAuth


class SUS_API:
    def __init__(self):
        self.username = os.getenv('USERNAME')
        self.password = os.getenv('PASSWORD')
        self.base_url = 'https://imunizacao-es.saude.gov.br/_search'

    def get_api(self):
        response = requests.get(self.base_url, auth=HTTPBasicAuth(self.username, self.password))
        return response.text