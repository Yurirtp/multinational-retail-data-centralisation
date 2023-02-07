#%%
import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self):
        self.connector = DatabaseConnector()
        
    def extract_user_data(self):
        table_name = "sales_data"
        df = self.read_rds_table(table_name)
        return df
        
    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=self.connector.engine)
        return df



