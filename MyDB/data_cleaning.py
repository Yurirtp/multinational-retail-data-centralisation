#%%
import pandas as pd
from datetime import datetime

class DataCleaning:
    def clean_user_data(self,df):
        # Filter DataFrame to include only users from specific countries
        a = ['United Kingdom', 'United States', 'Germany']
        cleaned_users = df[df.country.isin(a)].set_index('index')
        cleaned_users.reset_index(drop=True, inplace=True)
        # Replace country_code value
        cleaned_users['country_code'].replace(to_replace='GGB', value='GB', inplace=True)
        # Standardize phone numbers to 10 digits
        phone_series = cleaned_users['phone_number']
        phone_series2 = phone_series.apply(lambda x: ''.join([i for i in x if str.isnumeric(i)])[-10:])
        cleaned_users['phone_number'] = phone_series2    
        # Convert date strings to datetime format
        cleaned_users['date_of_birth'] = pd.to_datetime(cleaned_users['date_of_birth'])
        cleaned_users['join_date'] = pd.to_datetime(cleaned_users['join_date'])
        # Remove rows with missing values and where value column is equal to 0
        cleaned_users.dropna(inplace=True)
        #cleaned_users = cleaned_users[cleaned_users["value"] != 0]
        return cleaned_users

    def clean_card_data(self, card_data):
        '''Card Provider cleaning'''
        b = ['VISA 16 digit','JCB 16 digit','VISA 13 digit','JCB 15 digit', 'VISA 19 digit', 'Diners Club / Carte Blanche', 'American Express', 'Maestro', 'Discover', 'Mastercard']
        card_data = card_data[card_data.card_provider.isin(b)]
        # '''Clean expiry_date col'''
        # card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], format='%m/%y', errors='coerce')
        # # Filter out rows where 'expiry_date' is not a valid date
        # card_data = card_data.loc[card_data['expiry_date'].notnull()]
        # # Convert 'expiry_date' column back to the 'MM/YY' format
        # card_data['expiry_date'] = card_data['expiry_date'].dt.strftime('%m/%y')
        '''Card number cleaning'''
        null_map = card_data.applymap(lambda x: isinstance(x, (int, float)))['card_number']
        card_data_cleaned = card_data.iloc[null_map.values]
        return card_data_cleaned












#class DataCleaning:
    # def clean_user_data(self, df):
    #     #cleaning user data, import data frame and do pandas cleaning
    #     # Perform data cleaning operations, such as handling missing values, converting data types, etc.
    #     a=['United Kingdom','United States','Germany']
    #     cleaned_users = df[df.country.isin(a)].set_index('index')
    #     cleaned_users.reset_index(drop=True, inplace=True)
    #     cleaned_users['country_code'].replace(to_replace='GGB',value='GB', inplace=True)
    #     # Update phone number to 10-digits

    #     phone_series = cleaned_users['phone_number']
    #     phone_series2 = phone_series.apply(lambda x: ''.join([i for i in x if str.isnumeric(i)])[-10:])
    #     cleaned_users['phone_number'] = phone_series2
    #     # Join date to datetime converts DOB

    #     cleaned_users['date_of_birth'] = pd.to_datetime(cleaned_users['date_of_birth'])
    #     cleaned_users['join_date'] = pd.to_datetime(cleaned_users['join_date'])
        
        
        
        
        
        # df.dropna(inplace=True)
        # df["date"] = pd.to_datetime(df["date"])
        # df = df[df["value"] != 0]
        # return df