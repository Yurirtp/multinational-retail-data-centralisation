#%%
import yaml
import sqlalchemy as db

class DatabaseConnector:
    def __init__(self):
        self.creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        with open("db_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)
        return creds

    def init_db_engine(self):
        credentials = f"postgresql://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}"
        engine = db.create_engine(credentials)
        return engine

    def list_db_tables(self):
        metadata = db.MetaData()
        tables = db.Table("", metadata, autoload=True, autoload_with=self.engine)
        return tables.keys()

    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, con=self.engine, if_exists="replace", index=False)


