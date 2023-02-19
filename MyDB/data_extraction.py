#%%
import pandas as pd
import tabula
from database_utils import DatabaseConnector

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




