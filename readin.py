import json
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

def main():
    #API for EconDB
    API = '46c24fbb4887bf33205204f709392879bdae6177'
    lay_off_data = read_econdb(API)
    unemployment_data=read_unemployment()
    cpi_data=read_cpi()
    ir_marketable_data = read_ir_marketable()
    ir_non_marketable_data = read_ir_non_marketable()

if __name__ == "__main__":
    main()
    