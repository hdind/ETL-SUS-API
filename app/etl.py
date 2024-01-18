import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from google.cloud import storage
from api.sus import SUS_API


class ETL: 
    def extract():
        conn = SUS_API(os.getenv('USERNAME'), os.getenv('PASSWORD'))
        response = conn.get_api()
        return response.json()

    def transform(data):
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
            return df_raw
        
    def load_to_bucket(df, client):
        df.to_csv(r'..\data\sus_data.csv', index=False)

        bucket = client.get_bucket('stack-sus')
        blob = bucket.blob('/sus_data.csv')
        blob.upload_from_filename(r'..\data\sus_data.csv')

        print('Tudo certo, arquivo armazenado no bucket!')

    # def load_to_nosql(df):
        
    # def load_to_sb(df):


def main():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.\utils\coastal-fiber-411610-ff4ba4c97db9.json'
    client = storage.Client()

    sus_api = ETL()
    
    data = sus_api.extract()
    df_raw = sus_api.transform(data)
    sus_api.load_to_bucket(df_raw, client)

if __name__ == '__main__':
    main()
