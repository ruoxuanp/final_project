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


def main():
    #API for EconDB
    API = '46c24fbb4887bf33205204f709392879bdae6177'
    d = read_econdb(API)
    print(d)

   

    

if __name__ == "__main__":
    main()
    