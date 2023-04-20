import sqlite3
import json
import os
import requests
import re


def read_econdb(api): #read in layoff rate from EconDB

    # Make API request to fetch data
    url3 = f'https://www.econdb.com/api/series/JLRUS/?token={api}&format=json'
    response = requests.get(url3)

    # Load JSON data from the response
    data = json.loads(response.text)

    # Access the 'data' field from the JSON
    data = data['data']

    # Convert the data to a list of dictionaries
    list_of_dicts = []
    values = data["values"]
    dates = data["dates"]

    # Create a list of dictionaries
    list_of_dicts = []

    # Iterate over values and dates lists, and create a dictionary for each record
    for i in range(len(values)):
        dict_record = {
           'date': dates[i],
          'value': values[i]
        }
        list_of_dicts.append(dict_record)

    # Filter the data from year 2001 to 2022
    filtered = [d for d in list_of_dicts if "2001" <= d['date'][:4] <= "2022"]
    # keep year and month only
    for dic in filtered:
        dic['date'] = dic['date'][0:7]

    return filtered

def read_unemployment():
    # collect data for unemployment rate 
    url = 'https://www.alphavantage.co/query?function=UNEMPLOYMENT&apikey=8A43BBMGTP1CUXUK'
    r1 = requests.get(url)
    data_unemploy = r1.json()
    month_unemploy = data_unemploy['data'] 
    # filter the data in the range from 2001 to 2022
    filtered_month_unemploy = [d for d in month_unemploy if "2001" <= d['date'][:4] <= "2022"]
    
    for dic in filtered_month_unemploy:
        dic['date'] = dic['date'][0:7]
    filtered_month_unemploy.reverse()
    return(filtered_month_unemploy)


def read_cpi():
    url2 = 'https://www.alphavantage.co/query?function=CPI&interval=monthly&apikey=8A43BBMGTP1CUXUK'
    r2 = requests.get(url2)
    data_cpi = r2.json()
    month_cpi = data_cpi['data']

    # filter the data in the range from 2001 to 2022
    filtered_month_cpi = [d for d in month_cpi if "2001" <= d['date'][:4] <= "2022"]
    
    for dic in filtered_month_cpi:
        dic['date'] = dic['date'][0:7]
    
    filtered_month_cpi.reverse()
    return(filtered_month_cpi)

def read_ir_marketable():
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates"
    params = {
        "fields": "record_date,security_desc,avg_interest_rate_amt",
        "page[number]": 1,
        "page[size]": 100
    }

    all_data = []

    while True:
        r = requests.get(url, params=params)
        parsed_data = r.json()

        data_list = parsed_data['data']
        all_data.extend(data_list)

        # Check if there's a next page
        if not parsed_data['links']['next'] == None:
            params['page[number]'] += 1
        else:
            break

    l = []
    date_re = '(\d{4}-\d{2})-\d{2}'

    for item in all_data:
        if item['security_desc'] == 'Total Marketable':
            inner_d = dict()
            ym_data = re.findall(date_re, item['record_date'])
            for i in ym_data:
                inner_d['date'] = i

            inner_d['avg_interest_rate_amt'] = item['avg_interest_rate_amt']
            l.append(inner_d)
    
    filtered_l = [d for d in l if "2001" <= d['date'][:4] <= "2022"]

    return filtered_l

def read_ir_non_marketable():
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates"
    params = {
        "fields": "record_date,security_desc,avg_interest_rate_amt",
        "page[number]": 1,
        "page[size]": 100
    }

    all_data = []

    while True:
        r = requests.get(url, params=params)
        parsed_data = r.json()

        data_list = parsed_data['data']
        all_data.extend(data_list)

        # Check if there's a next page
        if not parsed_data['links']['next'] == None:
            params['page[number]'] += 1
        else:
            break

    l = []
    date_re = '(\d{4}-\d{2})-\d{2}'

    for item in all_data:
        if item['security_desc'] == 'Total Non-marketable':
            inner_d = dict()
            ym_data = re.findall(date_re, item['record_date'])
            for i in ym_data:
                inner_d['date'] = i

            inner_d['avg_interest_rate_amt'] = item['avg_interest_rate_amt']
            l.append(inner_d)
    
    filtered_l = [d for d in l if "2001" <= d['date'][:4] <= "2022"]

    return filtered_l


def setUpDatabase(db_name): # function to set up database
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def create_layoff_table(cur,conn,df): 

    # create Layoff_Rate table in database
    cur.execute("CREATE TABLE IF NOT EXISTS Layoff_Rate(id INTEGER PRIMARY KEY, date TEXT, layoff_rate NUMBER)")
    conn.commit()

def add_to_layoff_table(cur,conn,df):
    cur.execute("SELECT MAX(id) FROM Layoff_Rate")
    result = cur.fetchone()
    if result[0] is None:
        # if there are no entries in the table, set the starting ID to 0
        id = 0
    else:
        id = result[0]
    
    if id < 264:
        id += 1

        for i in range((id-1),(id+21)):
            date = df[i]['date']
            layoff = df[i]['value']
            intkey = i+1
            cur.execute("""INSERT INTO Layoff_Rate (id, date, layoff_rate)VALUES (?, ?, ?)""", (intkey, date, layoff))
        # Insert the entries into the 'Layoff_Rate' table
        #commit changes
        conn.commit()
    else:
        print("Layoff_Rate Finished")


def create_ir_marketable_table(cur,conn,df): 

    # create Layoff_Rate table in database
    cur.execute("CREATE TABLE IF NOT EXISTS IR_Marketable (id INTEGER PRIMARY KEY, date TEXT, avg_interest_rate_amt NUMBER)")
    conn.commit()

def add_to_ir_marketable_table(cur,conn,df):
    cur.execute("SELECT MAX(id) FROM IR_Marketable")
    result = cur.fetchone()
    if result[0] is None:
        # if there are no entries in the table, set the starting ID to 0
        id = 0
    else:
        id = result[0]

    if id < 264:
        id += 1

        for i in range((id-1),(id+21)):
            date = df[i]['date']
            avg_ir = df[i]['avg_interest_rate_amt']
            intkey = i+1
            cur.execute("""INSERT INTO IR_Marketable (id, date, avg_interest_rate_amt) VALUES (?, ?, ?)""", (intkey, date, avg_ir))
        # Insert the entries into the 'Layoff_Rate' table
        #commit changes
        conn.commit()
    else:
        print("IR_Marketable Finished")

def create_ir_non_marketable_table(cur,conn,df): 

    # create Layoff_Rate table in database
    cur.execute("CREATE TABLE IF NOT EXISTS IR_Non_Marketable (id INTEGER PRIMARY KEY, date TEXT, avg_interest_rate_amt NUMBER)")
    conn.commit()

def add_to_ir_non_marketable_table(cur,conn,df):
    cur.execute("SELECT MAX(id) FROM IR_Non_Marketable")
    result = cur.fetchone()
    if result[0] is None:
        # if there are no entries in the table, set the starting ID to 0
        id = 0
    else:
        id = result[0]

    if id < 264:
        id += 1

        for i in range((id-1),(id+21)):
            date = df[i]['date']
            avg_ir = df[i]['avg_interest_rate_amt']
            intkey = i+1
            cur.execute("""INSERT INTO IR_Non_Marketable (id, date, avg_interest_rate_amt) VALUES (?, ?, ?)""", (intkey, date, avg_ir))
        # Insert the entries into the 'Layoff_Rate' table
        #commit changes
        conn.commit()
    else:
        print("IR_Non_Marketable Finished")

def create_unemployment_table(cur,conn,df): 

    # create Layoff_Rate table in database
    cur.execute("CREATE TABLE IF NOT EXISTS Unemployment(id INTEGER PRIMARY KEY, date INTEGER, unemployment NUMBER)")
    conn.commit()

def add_to_unemployment_table(cur,conn,df):
    cur.execute("SELECT MAX(id) FROM Unemployment")
    result = cur.fetchone()
    if result[0] is None:
        # if there are no entries in the table, set the starting ID to 0
        id = 0
    else:
        id = result[0]
    
    if id < 264:
        id += 1
        for i in range((id-1),(id+21)):
            date = df[i]['date']
            unemployment = df[i]['value']
            intkey = i+1
            cur.execute("""INSERT INTO Unemployment (id, date, unemployment)VALUES (?, ?, ?)""", (intkey, date, unemployment))
        # Insert the entries into the 'Layoff_Rate' table
        #commit changes
        conn.commit()
    else:
        print("Unemployment Finished")

def create_cpi_table(cur,conn,df): 

    # create Layoff_Rate table in database
    cur.execute("CREATE TABLE IF NOT EXISTS Cpi_Rate(id INTEGER PRIMARY KEY, date INTEGER, cpi NUMBER)")
    conn.commit()

def add_to_cpi_table(cur,conn,df):
    cur.execute("SELECT MAX(id) FROM Cpi_Rate")
    result = cur.fetchone()
    if result[0] is None:
        # if there are no entries in the table, set the starting ID to 0
        id = 0
    else:
        id = result[0]
    
    if id < 264:
        id += 1
        for i in range((id-1),(id+21)):
            date = df[i]['date']
            cpi = df[i]['value']
            intkey = i+1
            cur.execute("""INSERT INTO Cpi_Rate (id, date, cpi)VALUES (?, ?, ?)""", (intkey, date, cpi))
        # Insert the entries into the 'Layoff_Rate' table
        #commit changes
        conn.commit()
    else:
        print("Cpi_Rate Finished")

def main():
    #API for EconDB
    API = '46c24fbb4887bf33205204f709392879bdae6177'
    lay_off_data = read_econdb(API)
    unemployment_data=read_unemployment()
    cpi_data=read_cpi()
    ir_marketable_data = read_ir_marketable()
    ir_non_marketable_data = read_ir_non_marketable()

    # set up database
    cur, conn = setUpDatabase('umemployment_data.db')

    create_layoff_table(cur,conn,lay_off_data)
    add_to_layoff_table(cur,conn,lay_off_data)
    create_ir_marketable_table(cur,conn,ir_marketable_data)
    add_to_ir_marketable_table(cur,conn,ir_marketable_data)
    create_ir_non_marketable_table(cur,conn,ir_non_marketable_data)
    add_to_ir_non_marketable_table(cur,conn,ir_non_marketable_data)
    create_unemployment_table(cur,conn,unemployment_data)
    add_to_unemployment_table(cur,conn,unemployment_data)
    create_cpi_table(cur,conn,cpi_data)
    add_to_cpi_table(cur,conn,cpi_data)
    

if __name__ == "__main__":
    main()
    