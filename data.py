import sqlite3
import json
import os
import requests
import pandas as pd
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
           'dates': dates[i],
          'values': values[i]
        }
        list_of_dicts.append(dict_record)

    # Filter the data from year 2001 to 2022
    filtered = [d for d in list_of_dicts if "2001" <= d['dates'][:4] <= "2022"]

    return filtered

def read_umempoylemnt():
    # collect data for umempoylemnt rate 
    url = 'https://www.alphavantage.co/query?function=NONFARM_PAYROLL&apikey=8A43BBMGTP1CUXUK'
    r1 = requests.get(url)
    data_umemploy = r1.json()
    month_umemploy = data_umemploy['data'] 
    # filter the data in the range from 2001 to 2022
    filtered_month_umemploy = [d for d in month_umemploy if "2001" <= d['date'][:4] <= "2022"]
    return(filtered_month_umemploy)

def read_cpi():
    url2 = 'https://www.alphavantage.co/query?function=CPI&interval=monthly&apikey=8A43BBMGTP1CUXUK'
    r2 = requests.get(url2)
    data_cpi = r2.json()
    month_cpi = data_cpi['data'] 

    # filter the data in the range from 2001 to 2022
    filtered_month_cpi = [d for d in month_cpi if "2001" <= d['date'][:4] <= "2022"]

    return(filtered_month_cpi)

def setUpDatabase(db_name): # function to set up database
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def create_layoff_table(cur,conn,df): 

    # create Layoff_Rate table in database
    cur.execute("DROP TABLE IF EXISTS Layoff_Rate")
    cur.execute("CREATE TABLE Layoff_Rate (date TEXT, layoff_rate NUMBER)")

    # Insert the entries into the 'Layoff_Rate' table
    for dict in df:
        date = str(dict['dates'])
        layoff = dict['values']

        cur.execute("""INSERT INTO Layoff_Rate (date, layoff_rate)VALUES (?, ?)""", (date, layoff))

    #commit changes
    conn.commit()

def main():
    #API for EconDB
    API = '46c24fbb4887bf33205204f709392879bdae6177'
    lay_off_data = read_econdb(API)
    print(lay_off_data)
    umempoylemnt_data=read_umempoylemnt()
    cpi_data=read_cpi()
    print(cpi_data)

    # set up database
    cur, conn = setUpDatabase('umemployment_data.db')
    # create Layoff_Rate table
    create_layoff_table(cur,conn,lay_off_data)
   

    

if __name__ == "__main__":
    main()
    