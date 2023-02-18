import pandas as pd

class DataCleaning:
    def clean_user_data(self, df):
        #cleaning user data, import data frame and do pandas cleaning
        # Perform data cleaning operations, such as handling missing values, converting data types, etc.
        df.dropna(inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["value"] != 0]
        return df