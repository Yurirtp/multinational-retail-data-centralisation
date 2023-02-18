#%%
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import data_cleaning as dc
import data_extraction as de
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd

def get_user_table(db_yaml_file):
    db_connector = DatabaseConnector(db_creds=db_yaml_file)
    data_extractor = DataExtractor(db_connector)
    user_db = data_extractor.read_rds_table('legacy_users')
    return user_db


def upload_user_table(db_creds_file, local_creds_file, data, table_name):
    db_conn = DatabaseConnector(db_creds_file)
    data_extractor = DataExtractor(db_conn)
    db_data = data_extractor.read_rds_table('legacy_users')
    cleaner = DataCleaning()
    cleaned_data = cleaner.clean_user_data(df=db_data)
    cleaned_df = pd.DataFrame(cleaned_data)
    db_conn = DatabaseConnector(local_creds_file)
    db_conn.upload_to_db(cleaned_df, table_name)
    print("User data has been uploaded successfully!")



db_creds_file = 'C:\\Users\\yurir\\Desktop\\AiCore\\Projects\\Github\\multinational-retail-data-centralisation\\MyDB\\db_creds.yaml'
local_creds_file = 'C:\\Users\\yurir\\Desktop\\AiCore\\Projects\\Github\\multinational-retail-data-centralisation\\MyDB\\local_db.yaml'
table_name = 'dim_users'
# Load user data from AWS cloud into a DataFrame
user_data = get_user_table(db_creds_file)
upload_user_table(db_creds_file, local_creds_file, user_data, table_name)


# def upload_user_table(local_yaml_file,data, table_name):
# local_creds = DatabaseConnector(db_creds=local_yaml_file).read_db_creds()
# #local_engine = DatabaseConnector(db_creds=local_yaml_file).init_db_engine()
# DatabaseConnector(local_creds).upload_to_db(data , table_name)  

# def upload_clean_user_data():
#     db_yaml_file = 'C:\\Users\\yurir\\Desktop\\AiCore\\Projects\\Github\\multinational-retail-data-centralisation\\MyDB\\db_creds.yaml'
#     local_yaml_file = 'C:\\Users\\yurir\\Desktop\\AiCore\\Projects\\Github\\multinational-retail-data-centralisation\\MyDB\\local_db.yaml'
#     user_data = get_user_table(db_yaml_file)
#     cleaner = DataCleaning()
#     user_data = cleaner.clean_user_data(df=user_data)
#     data_df = pd.DataFrame(user_data)
#     upload_user_table(local_yaml_file=local_yaml_file, data = data_df, table_name='dim_users')
#     print("User data has been uploaded successfully!")

# def upload_clean_user_data():
#     db_yaml_file = 'C:\\Users\\yurir\\Desktop\\AiCore\\Projects\\Github\\multinational-retail-data-centralisation\\MyDB\\db_creds.yaml'
#     local_yaml_file = 'C:\\Users\\yurir\\Desktop\\AiCore\\Projects\\Github\\multinational-retail-data-centralisation\\MyDB\\local_db.yaml'
#     user_data = get_user_table(db_yaml_file)
#     cleaner = DataCleaning()
#     user_data = cleaner.clean_user_data(df=user_data)
#     data_df = pd.DataFrame(data_dict)
#     upload_user_table(local_yaml_file=local_yaml_file, data=user_data, table_name='dim_users')
#     print("User data has been uploaded successfully!")
# db_yaml_file='db_creds.yaml'
# local_yaml_file='local_db.yaml'

# def get_user_table(db_yaml_file):
#     db_connector = DatabaseConnector(db_creds=db_yaml_file)
#     data_extractor = DataExtractor(db_connector)
#     user_db = data_extractor.read_rds_table('legacy_users')
#     return user_db

# # def get_user_table(db_yaml_file):
# #     '''returns a dataframe of the "legacy_users" table from the RDS database'''
# #     creds = DatabaseConnector(db_creds=db_yaml_file).read_db_creds()
# #     engine = DatabaseConnector(db_creds=db_yaml_file).init_db_engine()
# #     user_db = DataExtractor.read_rds_table(engine,'legacy_users')
# #     return user_db

# def upload_user_table(local_yaml_file,data, table_name):
#     '''converts dataframe to SQL and uploads to local PostGres server'''
#     local_creds = DatabaseConnector(db_creds=local_yaml_file).read_db_creds()
#     local_engine = DatabaseConnector(db_creds=local_yaml_file).init_db_engine()
#     DatabaseConnector(local_creds).upload_to_db(data , table_name)    


# def upload_clean_user_data():
#     user_data = get_user_table(db_yaml_file)
#     cleaner = DataCleaning()
#     user_data = cleaner.clean_user_data(df=user_data)
#     upload_user_table(local_yaml_file=local_yaml_file, data=user_data, table_name='dim_users')
#     print("User data has been uploaded successfully!")

# # def upload_clean_user_data():
# #     user_data = get_user_table(db_yaml_file)
# #     user_data = DataCleaning.clean_user_data(df=user_data)
# #     upload_user_table(local_yaml_file=local_yaml_file, data=user_data, table_name='dim_users')
# #     print("User data has been uploaded successfully!")

# # def upload_clean_user_data():
# #     user_data = get_user_table(db_yaml_file)
# #     user_data = DataCleaning.clean_user_data(user_data)
# #     upload_user_table(local_yaml_file=local_yaml_file,data=user_data,table_name='dim_users')

# upload_clean_user_data()

# db_yaml_file='db_creds.yaml'
# local_yaml_file='local_db.yaml'

# def get_user_table(db_yaml_file):
#     '''returns a dataframe of the "legacy_users" table from the RDS database'''
#     creds = DatabaseConnector.read_db_creds(db_yaml_file)
#     engine = DatabaseConnector.init_db_engine(creds)
#     user_db = DataExtractor.read_rds_table(engine,'legacy_users')
#     return user_db

# def upload_user_table(local_yaml_file,data, table_name):
#     '''converts dataframe to SQL and uploads to local PostGres server'''
#     local_creds = DatabaseConnector.read_db_creds(local_yaml_file)
#     local_engine = DatabaseConnector.init_db_engine(local_creds)
#     DatabaseConnector.upload_to_db(local_engine, data , table_name)    

# def upload_clean_user_data():
#     user_data = get_user_table(db_yaml_file)
#     user_data = DataCleaning.clean_user_data(user_data)
#     upload_user_table(local_yaml_file=local_yaml_file,data=user_data,table_name='dim_users')

# upload_clean_user_data()


# # Step 1: Extract data from RDS database
# dab = DatabaseConnector('db_creds.yaml')
# dab.init_db_engine()
# #List the tables
# tables = dab.list_db_tables()
# de = DataExtractor()
# original_df = de.extract_user_data()
# legacy_users = de.read_rds_table('legacy_users')
#if __name__ == "__main__":
    
    #de = DataExtractor()#
    #original_df = de.extract_user_data()#

    #Step 2: Clean the data
    #dc = DataCleaning()#
    #print(original_df.head())#
    #cleaned_df = dc.clean_user_data(original_df)

    # Step 3: Upload the cleaned data to Sales_Data database
    # connector = DatabaseConnector("local_db.yaml")
    # connector.upload_to_db(cleaned_df, 'dim_users')


# pip install --force-reinstall -v SQLAlchemy==1.4.46
#create a new enviornment