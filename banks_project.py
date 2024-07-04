# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing.'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required information from the website and save it to a dataframe. The function returns the dataframe for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    
    # Print the page content to understand its structure
    print(data.prettify())
    
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('table')
    
    print(f"Found {len(tables)} tables")
    
    # Assuming the required table is the first one (index 0), update as necessary
    table = tables[0]
    rows = table.find_all('tr')
    
    print(f"Found {len(rows)} rows in the table")
    
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:  # Ensure there are at least 3 elements
            if col[0].find('a') is not None and '—' not in col[2].text:
                data_dict = {"Country": col[0].a.contents[0], "GDP_USD_millions": col[2].text}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    
    return df

def transform(df, csv_path):
    ''' This function converts the GDP information from Currency format to float value, transforms the information of GDP from USD (Millions) to USD (Billions) rounding to 2 decimal places. The function returns the transformed dataframe. '''
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x / 1000, 2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file in the provided path. Function returns nothing. '''
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to a database table with the provided name. Function returns nothing. '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Main ETL Process
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Country", "GDP_USD_millions"]
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

# Extract data
df = extract(url, table_attribs)

# Check the number of rows extracted
print("Number of rows extracted:", len(df))

# Transform data
df = transform(df, csv_path)

# Load data to CSV
load_to_csv(df, csv_path)

# Load data to database
sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)

# Exchange rate data
exchange_rate_df = pd.read_csv('exchange_rate.csv')
exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

# Add additional columns
df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['GDP_USD_billions']]
df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['GDP_USD_billions']]
df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['GDP_USD_billions']]

# Print specific value for quiz if there are enough rows
if len(df) > 4:
    print("Market capitalization of the 5th largest bank in billion EUR:", df['MC_EUR_Billion'][4])
else:
    print("There are not enough rows in the DataFrame to print the 5th largest bank's market capitalization in billion EUR.")

# Run a sample query
query = "SELECT * FROM {}".format(table_name)
run_query(query, sql_connection)

# Close the database connection
sql_connection.close()
