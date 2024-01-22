import requests
from requests.auth import HTTPBasicAuth


class SUS_API:
    """
    Classe para interagir com a API do SUS.

    Attributes:
        username (str) -> Nome de usuário para autenticação na API.
        password (str) -> Senha para autenticação na API.
        base_url (str) -> URL base da API.

    Methods:
        get_api() -> Realiza uma solicitação GET à API do SUS.
    """

    def __init__(self, username, password):
        """
        Inicializa a instância da classe SUS_API.

        Args:
            username (str) -> Nome de usuário para autenticação na API.
            password (str) -> Senha para autenticação na API.
        """
        
        self.username = username
        self.password = password
        self.base_url = 'https://imunizacao-es.saude.gov.br/_search'

    def get_api(self):
        """
        Realiza uma solicitação GET à API do SUS.

        Returns:
            requests.Response -> Objeto de resposta da solicitação à API.
        Raises:
            Exception -> Se a resposta da API não retornar um código de status 200.
        """

        print('requisição iniciada')
        try:
            response = requests.get(self.base_url, auth=HTTPBasicAuth(self.username, self.password))
            
            if response.status_code == 200:
                return response
            else:
                print(f'Erro na solicitação!!! Código de Status: {response.status_code}')
                raise Exception
            
        except Exception as e:
            print(f'Erro ao acessar a API: {str:(e)}')    


if __name__ == '__main__':
    # Exemplo de uso da classe SUS_API
    conn = SUS_API()
    print(conn.get_api())