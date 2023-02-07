#%%
import yaml
import sqlalchemy as db

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
        return engine

    def list_db_tables(self):
        inspector = db.inspect(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, con=self.engine, if_exists="replace", index=False)


#db = DatabaseConnector("local_db.yaml")
#print(db.creds)


