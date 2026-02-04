import requests
import pandas as pd 
import numpy as np 
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3
import os  


# Get the directory where THIS script is currently sitting
script_dir = os.path.dirname(os.path.abspath(__file__))

# Join that directory with your file names to create bulletproof absolute paths
output_path = os.path.join(script_dir, 'largest_bank_data.csv')
log_file = os.path.join(script_dir, 'code_log.txt')
db_name = os.path.join(script_dir, 'Banks.db')  # Also fixed DB path just in case

table_name = 'Largest_banks'
table_attribs = ['Name', 'MC_USD_Billions']
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

# --- REST OF YOUR CODE BELOW ---

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + " : " + message + '\n')

def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    data_list = []
    count = 0
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 and count < 10:  
            link = col[1].find_all('a')
            if link:
                bank_name = link[1].get_text(strip=True) 
                bank_name = col[1].get_text(strip=True)
                
            market_cap = col[2].get_text(strip=True)
            
            
            market_cap = float(market_cap.replace('\n', ''))

            data_dict = {"Name": bank_name, "MC_USD_Billions": market_cap}
            data_list.append(data_dict)
            count += 1
            
    df = pd.DataFrame(data_list, columns=table_attribs)
    log_progress('Data extraction complete. Initiating Transformation process')
    return df

def transform(df, csv_path):
    exchanged_df = pd.read_csv(csv_path)
    
    
    exchanged_rate = exchanged_df.set_index('Currency').to_dict()['Rate']
    
    df['MC_GBP_Billions'] = round(df['MC_USD_Billions'] * exchanged_rate['GBP'], 2)
    df['MC_EUR_Billions'] = round(df['MC_USD_Billions'] * exchanged_rate['EUR'], 2)
    df['MC_INR_Billions'] = round(df['MC_USD_Billions'] * exchanged_rate['INR'], 2)
    
    log_progress('Data transformation complete. Initiating Loading process')    
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    print(f"\nQuery: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# --- EXECUTION ---
# 1. Extract
df = extract(url, table_attribs)

# 2. Transform
df_transformed = transform(df, csv_path)

# 3. Load to CSV
load_to_csv(df_transformed, output_path)

# 4. Load to Database
sql_connection = sqlite3.connect(db_name)
load_to_db(df_transformed, sql_connection, table_name)

# 5. Run Queries
query_statement_1 = "SELECT * FROM Largest_banks"
query_statement_2 = "SELECT AVG(MC_GBP_Billions) FROM Largest_banks"
query_statement_3 = "SELECT Name from Largest_banks LIMIT 5"

run_query(query_statement_1, sql_connection)
run_query(query_statement_2, sql_connection)
run_query(query_statement_3, sql_connection)

log_progress('Process Complete')

# 6. Close Connection
sql_connection.close()

print(f"\nSUCCESS! Files saved to: {script_dir}")