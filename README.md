# Projeto ETL SUS

Este é um projeto Python que realiza operações de Extração, Transformação e Carga (ETL) de dados da API do SUS para o Google Cloud Storage.

## Estrutura do Projeto

O projeto está organizado da seguinte maneira:

- `api/`: Contém o módulo `sus.py` que define a classe `SUS_API` para interação com a API do SUS.
- `data/`: Diretório onde o arquivo CSV resultante da transformação dos dados será armazenado.
- `utils/`: Contém o arquivo de credenciais do Google Cloud Storage.
- `main.py`: Arquivo principal que executa as operações ETL.

## Pré-requisitos

- Python 3.11.6
- Bibliotecas Python: pandas, google-cloud-storage, google-cloud-firestore, requests, SQLAlchemy e pyarrow.

## Configuração

1. Instale as dependências executando o seguinte comando no terminal:

    ```bash
    pip install -r requirements.txt
    ```

2. Configure as variáveis de ambiente:

    - `SUS_API_USERNAME`: Nome de usuário para autenticação na API do SUS.
    - `SUS_API_PASSWORD`: Senha para autenticação na API do SUS.
    - `MYSQL_USERNAME`: Nome de usuário para autenticação na instância do MySQL no Google Cloud SQL.
    - `MYSQL_PASSWORD`: Senha para autenticação na instância do MySQL no Google Cloud SQL.
    - `GOOGLE_APPLICATION_CREDENTIALS`: Caminho para o arquivo de credenciais do Google Cloud Storage.

## Uso

Execute o script principal `main.py` para realizar a extração, transformação e carga dos dados:

```bash
python main.py