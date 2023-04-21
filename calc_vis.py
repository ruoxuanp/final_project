import sqlite3
import json
import os
import requests
import csv

def ConnectDatabase(db_name): # function to connect database
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def retrive_umemployment_lay0ff(cur,conn):
    cur.execute('''
    SELECT Unemployment.unemployment, Layoff_Rate.layoff_rate FROM Unemployment JOIN Layoff_Rate 
    ON Unemployment.id = Layoff_Rate.id
    ''')
    ratio = list()
    rows = cur.fetchall()
    for row in rows:
        layoff = row[1]
        umemployment = row[0]
        ratio.append(round((layoff/umemployment)*100,2))

    csv_file = "ratio.csv"

    # Open the CSV file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

    # Write the numbers to the CSV file
        writer.writerow(["Number"])  # Write header row
        for num in ratio:
            writer.writerow([num])  # Write each number as a row

    return (ratio)


def average_year_CPI(cur,conn):
    cur.execute('''SELECT cpi FROM Cpi_Rate''')
    rows = cur.fetchall()
    year_CPI = list()
    cpi = list()
    for row in rows:
        cpi.append(row[0])

    for i in range(0, len(cpi), 12):
        lists = cpi[i:i+12]
        sum = 0
        for item in lists:
            sum += item
        avg = round(sum/12,2)
        year_CPI.append(avg)

    return(year_CPI)

def retrieve_year():
    years = []
    for year in range(2013, 2023):
        years.append(year)
    
    return(years)

def avg_year_IR_Non_Marketable(cur,conn):
    cur.execute('''
    SELECT avg_interest_rate_amt FROM IR_Non_Marketable 
    ''')
    rows_rate_Non_Marketable  = cur.fetchall()
    rate_Non_Marketable = list() 
    average = list()
    for row in rows_rate_Non_Marketable:
        rate_Non_Marketable.append(row[0])
    
    for i in range(0, len(rate_Non_Marketable), 12):
        lists = rate_Non_Marketable[i:i+12]
        sum = 0
        for item in lists:
            sum += item
        avg = round(sum/12,2)
        average.append(avg)

    
    return(average)

    
def main():
    cur, conn = ConnectDatabase('umemployment_data.db')
    ratio = retrive_umemployment_lay0ff(cur,conn)
    #print(ratio)
    d = average_year_CPI(cur,conn)
    #print(d)
    year = retrieve_year()
    #print(year)
    IR = avg_year_IR_Non_Marketable(cur,conn)
    print(IR)
    pass


if __name__ == "__main__":
    main()