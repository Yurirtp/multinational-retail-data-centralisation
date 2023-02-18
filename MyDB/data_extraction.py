#%%
import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self):
        self.connector = DatabaseConnector("db_creds.yaml")

    def extract_user_data(self):
        table_name = "legacy_users"
        df = self.read_rds_table(table_name)
        return df

    #def retrieve_pdf_data(self, link):
        #df = tabula.read_pdf(link, multiple_tables=True)
        #return pd.concat(df)#
        
    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=self.connector.engine)
        return df




