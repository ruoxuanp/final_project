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
        dic['date'] = int(dic['date'].replace("-", ""))
    return filtered

def read_umempoylemnt():
    # collect data for umempoylemnt rate 
    url = 'https://www.alphavantage.co/query?function=NONFARM_PAYROLL&apikey=8A43BBMGTP1CUXUK'
    r1 = requests.get(url)
    data_umemploy = r1.json()
    month_umemploy = data_umemploy['data'] 
    # filter the data in the range from 2001 to 2022
    filtered_month_umemploy = [d for d in month_umemploy if "2001" <= d['date'][:4] <= "2022"]
    for dic in filtered_month_umemploy:
        dic['date'] = dic['date'][0:7]
        dic['date'] = int(dic['date'].replace("-", ""))
    return(filtered_month_umemploy)


def read_cpi():
    url2 = 'https://www.alphavantage.co/query?function=CPI&interval=monthly&apikey=8A43BBMGTP1CUXUK'
    r2 = requests.get(url2)
    data_cpi = r2.json()
    month_cpi = data_cpi['data'] 

    # filter the data in the range from 2001 to 2022
    filtered_month_cpi = [d for d in month_cpi if "2001" <= d['date'][:4] <= "2022"]
    for dic in filtered_month_cpi:
        dic['date'] = dic['date'][0:7]
        dic['date'] = int(dic['date'].replace("-", ""))
    return(filtered_month_cpi)

def read_avg_interest_rate():
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
        if item['security_desc'] == 'Total Marketable' or item['security_desc'] == 'Total Non-marketable':
            inner_d = dict()
            ym_data = re.findall(date_re, item['record_date'])
            for i in ym_data:
                inner_d['record_date'] = int(i.replace('-', ''))

            inner_d['security_desc'] = item['security_desc']
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
    cur.execute("DROP TABLE IF EXISTS Layoff_Rate")
    cur.execute("CREATE TABLE Layoff_Rate (date INTEGER PRIMARY KEY, layoff_rate NUMBER)")

    # Insert the entries into the 'Layoff_Rate' table
    for dict in df:
        date = dict['date']
        layoff = dict['value']

        cur.execute("""INSERT INTO Layoff_Rate (date, layoff_rate)VALUES (?, ?)""", (date, layoff))

    #commit changes
    conn.commit()

def main():
    #API for EconDB
    API = '46c24fbb4887bf33205204f709392879bdae6177'
    lay_off_data = read_econdb(API)
    umempoylemnt_data=read_umempoylemnt()
    cpi_data=read_cpi()

    # set up database
    cur, conn = setUpDatabase('umemployment_data.db')
    # create Layoff_Rate table
    create_layoff_table(cur,conn,lay_off_data)
   

    

if __name__ == "__main__":
    main()
    