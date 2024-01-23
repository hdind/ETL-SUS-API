import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pymysql
import pandas as pd
from google.cloud import storage, firestore
from api.sus import SUS_API
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector


class ETL: 
    """
    Classe para realizar operações de Extração, Transformação e Carga (ETL) de dados.
    
    Atributes: 
        None

    Methods:
        extract() -> Realiza extracação dos dados da API do SUS.
        transform() -> Transforma os dados brutos retornados em um df.
        load_to_bucket() -> Carrega um arquivo csv no Google Cloud Storage.
        load_to_mysql() -> Carrega um arquivo csv num banco MySQL no Google Cloud SQL.
        load_to_firestore() -> Carrega um arquivo csv num banco NoSQL no Google Cloud Firestore.
    """
   
    def extract():
        """
        Realiza a extração de dados da API do SUS.

        Returns:
            dict: Dados extraídos da API no formato de dicionário.
        """
        conn = SUS_API(os.getenv('SUS_API_USERNAME'), os.getenv('SUS_API_PASSWORD'))
        response = conn.get_api()
        return response.json()
    
    def transform(data:dict):
        """
        Realiza a transformação dos dados da API em um DataFrame e salva em um arquivo CSV.

        Args:
            data (dict): Dados extraídos da API no formato de dicionário.
        Returns:
            int: Retorna 1 para indicar que a transformação foi concluída.
        """

        df_raw = pd.DataFrame()

        print('Iniciando estruturação dos dados.')

        for i in range(len(data['hits']['hits'])):
            df_temp = pd.DataFrame()

            df_temp['paciente_idade'] = data['hits']['hits'][i]['_source']['paciente_idade'],
            df_temp['paciente_enumSexoBiologico'] = data['hits']['hits'][i]['_source']['paciente_enumSexoBiologico'],
            df_temp['paciente_racaCor_valor'] = data['hits']['hits'][i]['_source']['paciente_racaCor_valor'],
            df_temp['paciente_endereco_nmMunicipio'] = data['hits']['hits'][i]['_source']['paciente_endereco_nmMunicipio'],
            df_temp['paciente_endereco_uf'] = data['hits']['hits'][i]['_source']['paciente_endereco_uf'],
            df_temp['estabelecimento_razaoSocial'] = data['hits']['hits'][i]['_source']['estabelecimento_razaoSocial'],
            df_temp['vacina_fabricante_referencia'] = data['hits']['hits'][i]['_source']['vacina_fabricante_referencia'],
            df_temp['vacina_categoria_nome'] = data['hits']['hits'][i]['_source']['vacina_categoria_nome'],
            df_temp['vacina_lote'] = data['hits']['hits'][i]['_source']['vacina_lote'],
            df_temp['vacina_fabricante_nome'] = data['hits']['hits'][i]['_source']['vacina_fabricante_nome'],
            df_temp['vacina_dataAplicacao'] = data['hits']['hits'][i]['_source']['vacina_dataAplicacao']

            df_raw = pd.concat([df_raw, df_temp], ignore_index=True)

        df_raw.to_csv(r'.\data\sus_data.csv', index=False)
        print('Arquivo csv salvo.')
        df_raw.to_parquet(r'.\data\sus_data.parquet', compression='snappy', index=False)
        print('Arquivo parquet salvo.')
        return 1
        
    def load_to_bucket(client):
        """
        Carrega o arquivo CSV gerado para um bucket no Google Cloud Storage.

        Args:
            client: Cliente do Google Cloud Storage.

        Returns:
            int: Retorna 1 para indicar que o carregamento foi concluído.
        """
        print('Iniciando conexão com bucket.')
        bucket = client.get_bucket('stack-sus')

        blob = bucket.blob('sus_data.csv')
        blob.upload_from_filename(r'.\data\sus_data.csv')

        blob = bucket.blob('sus_data.parquet')
        blob.upload_from_filename(r'.\data\sus_data.parquet')

        print('Tudo certo, arquivos armazenados no bucket!')
        return 1
            
    def load_to_mysql():
        """
        Carrega dados de um arquivo CSV para uma tabela MySQL.

        Retorna:
        int: 1 se o carregamento for bem-sucedido.
        """
        
        print('Conectando com a instância MySQL.')
        connector = Connector()

        # function to return the database connection
        def getconn() -> pymysql.connections.Connection:
            """
            Retorna uma conexão com o banco de dados MySQL.

            Retorna:
            pymysql.connections.Connection: Conexão com o banco de dados.
            """
            
            conn: pymysql.connections.Connection = connector.connect(
                "coastal-fiber-411610:us-central1:teste-stack-01",
                "pymysql",
                user=os.getenv("MYSQL_USERNAME"),
                password=os.getenv("MYSQL_PASSWORD"),
                db="default"
            )
            return conn
        
        pool = create_engine(
            "mysql+pymysql://",
            creator=getconn,
        )

        with pool.connect() as db_conn:
            try:
                create_table = db_conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS covid_sus (
                        paciente_idade INT,
                        paciente_enumSexoBiologico VARCHAR(255),
                        paciente_racaCor_valor VARCHAR(255),
                        paciente_endereco_nmMunicipio VARCHAR(255),
                        paciente_endereco_uf VARCHAR(2),
                        estabelecimento_razaoSocial VARCHAR(255),
                        vacina_fabricante_referencia VARCHAR(255),
                        vacina_categoria_nome VARCHAR(255),
                        vacina_lote VARCHAR(255),
                        vacina_fabricante_nome VARCHAR(255),
                        vacina_dataAplicacao VARCHAR(50)
                    );    
                """))

                create_table.fetchall()
            except:
                pass

            df = pd.read_csv(r'.\data\sus_data.csv')
            df.to_sql('covid_sus', con=db_conn, if_exists='replace', index=False)
            db_conn.commit()
            print('Dados salvos no MySQL com sucesso!')
            return 1

    def load_to_firestore(firestore_client):
        """
        Carrega dados de um arquivo CSV para o Firestore.

        Args:
        firestore_client: Cliente Firestore.

        Retorna:
        int: 1 se o carregamento for bem-sucedido.
        """
        df = pd.read_csv(r'.\data\sus_data.csv')
        data = df.to_dict(orient='records')

        doc_ref = firestore_client.collection('covid_sus')

        for i, record in enumerate(data):
            doc_ref.document(f'record_{i}').set(record)
        print('Dados adicionados ao Firestore com sucesso!')

        return 1

def main():
    """
    Função principal que executa as operações ETL.
    """
    
    print('Criando clientes de conexão com GCP.')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.\utils\coastal-fiber-411610-ff4ba4c97db9.json'
    bucket_client = storage.Client()
    firestore_client = firestore.Client()

    sus_api = ETL
    
    data = sus_api.extract()
    sus_api.transform(data)
    sus_api.load_to_bucket(bucket_client)
    sus_api.load_to_mysql()
    sus_api.load_to_firestore(firestore_client)
    return 1

if __name__ == '__main__':
    main()
