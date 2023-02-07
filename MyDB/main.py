#%%

import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

if __name__ == "__main__":
    # Step 1: Extract data from RDS database
    de = DataExtractor()
    original_df = de.extract_user_data()

    # Step 2: Clean the data
    dc = DataCleaning()
    cleaned_df = dc.clean_user_data(original_df)

    # Step 3: Upload the cleaned data to Sales_Data database
    connector = DatabaseConnector()
    connector.upload_to_db(cleaned_df, 'dim_users')