import pandas as pd
import traceback
import sys
from datetime import datetime
import os
import numpy as np
import ftplib

input_file = 'C:\CSV_Split\person_licenses_certifications.csv'

# rename column dictionary
rename_dict = {"Employee #": "employee_id",
               "Qualification": "name",
               "License Number": "number",
               "License Issued Date": "start_date",
               "License Expiry Date": "expiration_date",
               }

                       
# settings dictionary: keys -> output path, and values -> list of columns
settings = {
    'person_licenses_certifications.csv': ['feed_type', 'feed_date', 'client_id', 'account_code', 'Employee #', 'type', 'Qualification',
                      'License Number', 'state', 'License Issued Date', 'License Expiry Date', 'Termination Date']
    }


def print_log(msg: str):
    """
    print message and writes to LOG.txt
    :param msg:
    :return:
    """
    try:
        print(msg)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("LOG.txt", 'a+') as f:
            f.write('[{}] {}\n'.format(now, msg))
    except Exception as e:
        print("Exception in print_log: {}".format(str(e)))
        print(traceback.format_exc())


def recreate_log_file():
    """
    recreats LOG.txt file
    :return:
    """
    try:
        # removing LOG file of previous session if exists
        if os.path.exists("LOG.txt"):
            os.remove("LOG.txt")
    except Exception as e:
        print_log("Except exception in recreate_log_file: {}".format(str(e)))
        print_log(traceback.format_exc())


def read_to_pd(path):
    """
    reads .csv or .xls fle to dataframe
    :param path:
    :return:
    """
    try:
        if '.csv' in path.lower():
            df = pd.read_csv(path)
        elif '.xls' in path.lower():
            df = pd.read_excel(path)
        else:
            print("Wrong file format: {}".format(path))
            sys.exit(0)
        return df

    except Exception as e:
        print(str(e))
        print(traceback.format_exc())


def main():
    """
    the main pipeline
    :return:
    """
    try:
        # recreating LOG file
        recreate_log_file()
        df = read_to_pd(input_file)
        
        #declare today
        today = pd.Timestamp.today()
        #add today's date to feed_date column
        df['feed_date'].replace(np.nan,np.datetime64('today'), inplace=True)
        #change Termination Date column values to datetime64
        df['Termination Date'] = pd.to_datetime(df['Termination Date'], errors='coerce')
        #remove rows where the termination date is less than today's date
        df.drop(df.index[(df["Termination Date"] <= np.datetime64('today'))], axis=0,inplace=True)
        #After above rows are removed, delete column entirely
        df = df.drop(columns=['Termination Date'])
        #Convert Employee # column to string
        df['Employee #'] = df['Employee #'].astype(str)
        #Remove rows with greater than 5 characters
        df = df.loc[df['Employee #'].str.len() != 5]
       
        print('!!!')
        
        print("Columns:")
        print_log(str(list(df.columns)))
        for file_name, columns in settings.items():
            columns_exists = [x for x in columns if x in df.columns]
            columns_missed = [x for x in columns if x not in df.columns]
            if columns_missed:
                print_log("!!! columns not detected: {}".format(str(columns_missed)))
            temp_df = df[columns_exists]

            for old, new in rename_dict.items():
                if old in list(temp_df.columns):
                    temp_df.rename(columns={old: new}, inplace=True)

           # if file_name in default_values_dict.keys():
           #     for column, value in default_values_dict[file_name].items():
           #         if column in temp_df.columns:
           #            temp_df[column] = value


       
            temp_df.to_csv(file_name, index=False)
            print_log("Saved to: {}".format(file_name))

    except Exception as e:
        print_log("Except exception in main: {}".format(str(e)))
        print_log(traceback.format_exc())



if __name__ == '__main__':
    # freeze support
    main()
