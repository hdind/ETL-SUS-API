import os
import requests
from requests.auth import HTTPBasicAuth


class SUS_API:
    def __init__(self):
        self.username = os.getenv('USERNAME')
        self.password = os.getenv('PASSWORD')
        self.base_url = 'https://imunizacao-es.saude.gov.br/_search'

    def get_api(self):
        try:
            response = requests.get(self.base_url, auth=HTTPBasicAuth(self.username, self.password))
            
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f'Erro na solicitação!!! Código de Status: {response.status_code}')
                return response.json()
            
        except Exception as e:
            print(f'Erro ao acessar a API: {str:(e)}')    

if __name__ == '__main__':
    conn = SUS_API()
    print(conn.get_api())