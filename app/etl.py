import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pymysql
import pandas as pd
from google.cloud import storage
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
    """
   
    def extract():
        """
        Realiza a extração de dados da API do SUS.

        Returns:
            dict: Dados extraídos da API no formato de dicionário.
        """
            
        conn = SUS_API(os.getenv('USERNAME'), os.getenv('PASSWORD'))
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
        print(f'{len(df_raw)} linhas foram salvas em .\data\sus_data.csv')
        return 1
        
    def load_to_bucket(client):
        """
        Carrega o arquivo CSV gerado para um bucket no Google Cloud Storage.

        Args:
            client: Cliente do Google Cloud Storage.

        Returns:
            int: Retorna 1 para indicar que o carregamento foi concluído.
        """

        bucket = client.get_bucket('stack-sus')
        blob = bucket.blob('sus_data.csv')
        blob.upload_from_filename(r'.\data\sus_data.csv')

        print('Tudo certo, arquivo armazenado no bucket!')
        return 1
            
    def load_to_mysql():
        connector = Connector()

        # function to return the database connection
        def getconn() -> pymysql.connections.Connection:
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
            print(df.info())
            df.to_sql('covid_sus', con=db_conn, if_exists='replace', index=False)
            db_conn.commit()
            return 1

    # def load_to_nosql(df):

    # def load_parquet(df):
        

def main():
    """
    Função principal que executa as operações ETL.
    """
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.\utils\coastal-fiber-411610-ff4ba4c97db9.json'
    client = storage.Client()

    sus_api = ETL
    
    # data = sus_api.extract()
    # sus_api.transform(data)
    # sus_api.load_to_bucket(client)
    sus_api.load_to_mysql()

if __name__ == '__main__':
    main()
