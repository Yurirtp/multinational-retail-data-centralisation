#%%
import yaml
import sqlalchemy as db
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DatabaseConnector:
    def __init__(self, db_creds):
        self.db_creds = db_creds
        self.creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        with open(self.db_creds, "r") as stream:
            creds = yaml.safe_load(stream)
        return creds    

    def init_db_engine(self):
        creds = self.creds
        credentials = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(credentials)
        engine.connect()
        return engine

    def list_db_tables(self):
        inspector = db.inspect(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, data, table_name: str):
        data.to_sql(table_name, con=self.engine, if_exists="replace", index=False)
        pass


# class DatabaseConnector:
#     def __init__(self, db_creds):
#         self.db_creds = db_creds
#         self.creds = self.read_db_creds()
#         self.engine = self.init_db_engine()
  

#     def read_db_creds(self):
#         with open(self.db_creds, "r") as stream:
#             creds = yaml.safe_load(stream)
#         return creds    

#     def init_db_engine(self):
#         creds = self.creds
#         credentials = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
#         engine = db.create_engine(credentials)
#         engine.connect()
#         return engine

#     def list_db_tables(self):
#         inspector = db.inspect(self.engine)
#         return inspector.get_table_names()

#     def upload_to_db(self, df, table_name):
#         df.to_sql(table_name, con=self.engine, if_exists="replace", index=False)
#         pass


#dab = DatabaseConnector("local_db.yaml")
#dab.read_db_creds()


