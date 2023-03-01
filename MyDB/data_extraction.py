#%%
import pandas as pd
import tabula
from database_utils import DatabaseConnector
import requests

class DataExtractor:
    def __init__(self, db_connector):
        self.connector = db_connector

    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=self.connector.engine)
        return df

    def extract_user_data(self):
        table_name = "legacy_users"
        df = self.read_rds_table(table_name)
        return df

    def retrieve_pdf_data(self, url: str):
        '''takes PDF url as input and outputs PDF data as table'''
        dfs =tabula.read_pdf(url,pages='all')
        card_data=pd.concat(dfs,ignore_index=True)
        # print('number of dataframes created:')
        # print(len(dfs))
        return card_data

    def list_number_of_stores(self):
        headers = {
            "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }
        response = requests.get("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", headers=headers)
        return response.json()["number_stores"]


    def retrieve_store_data(self,number_stores,retrieve_store_endpoint,header):
        '''take in endpoint and API_key and returns a dataframe of all stores information'''
        stores_data = []
        for i in range(number_stores):
            store_url = retrieve_store_endpoint+str(i)
            try:
                response = requests.get(store_url, headers=header)
            except:
                print("there was an error!")    
            stores_data.append(response.json())
        df = pd.DataFrame(stores_data)
        return df        


# class DataExtractor:
#     def __init__(self, db_connector):
#         self.connector = db_connector
#         db_connector = DatabaseConnector("db_creds.yaml")

#     def read_rds_table(self, table_name):
#         query = f"SELECT * FROM {table_name}"
#         df = pd.read_sql(query, con=self.connector.engine)
#         return df

#     def extract_user_data(self):
#         table_name = "legacy_users"
#         df = self.read_rds_table(table_name)
#         return df

    #def retrieve_pdf_data(self, link):
        #df = tabula.read_pdf(link, multiple_tables=True)
        #return pd.concat(df)#
        
    # def read_rds_table(self, table_name):
    #     query = f"SELECT * FROM {table_name}"
    #     df = pd.read_sql(query, con=self.connector.engine)
    #     return df




