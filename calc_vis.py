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

def retrive_umemployment_layoff(cur,conn):
    cur.execute('''
    SELECT Unemployment.unemployment, Unemployment.date, Layoff_Rate.layoff_rate FROM Unemployment JOIN Layoff_Rate 
    ON Unemployment.id = Layoff_Rate.id
    ''')
    ratio = list()
    rows = cur.fetchall()
    for row in rows:
        inner = list()
        umemployment = row[0]
        date = row[1]
        layoff = row[2]
        inner.append(date)
        inner.append(round((layoff/umemployment)*100,2))
        ratio.append(inner)

    csv_file = "ratio.csv"

    # Open the CSV file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

    # Write the numbers to the CSV file
        writer.writerow(["Date", "Ratio"])  # Write header row
        for num in ratio:
            writer.writerow(num)  # Write each number as a row

    return (ratio)
    


def average_year_CPI(cur,conn):
    cur.execute('''SELECT cpi FROM Cpi_Rate''')
    rows = cur.fetchall()
    average = list()
    cpi = list()
    year = 2013

    for row in rows:
        cpi.append(row[0])

    for i in range(0, len(cpi), 12):
        inner = list()
        lists = cpi[i:i+12]
        sum = 0
        for item in lists:
            sum += item
        avg = round(sum/12,2)
        inner.append(year)
        inner.append(avg)
        average.append(inner)
        year +=1
    
    csv_file = "year_average_CPI.csv"

    # Open the CSV file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

    # Write the numbers to the CSV file
        writer.writerow(["Year", "Average CPI"])  # Write header row
        for num in average:
            writer.writerow(num)  # Write each number as a row

    return(average)


def avg_year_IR_Non_Marketable(cur,conn):
    cur.execute('''
    SELECT avg_interest_rate_amt FROM IR_Non_Marketable 
    ''')
    rows_rate_Non_Marketable  = cur.fetchall()
    rate_Non_Marketable = list() 
    average = list()
    year = 2013
    for row in rows_rate_Non_Marketable:
        rate_Non_Marketable.append(row[0])
    
    for i in range(0, len(rate_Non_Marketable), 12):
        inner = list()
        lists = rate_Non_Marketable[i:i+12]
        sum = 0
        for item in lists:
            sum += item
        avg = round(sum/12,2)
        inner.append(year)
        inner.append(avg)
        average.append(inner)
        year +=1
    
    csv_file = "year_average_IR.csv"

    # Open the CSV file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

    # Write the numbers to the CSV file
        writer.writerow(["Year", "Average Interest Rate"])  # Write header row
        for num in average:
            writer.writerow(num)  # Write each number as a row

    
    return(average)


def main():
    cur, conn = ConnectDatabase('umemployment_data.db')
    ratio = retrive_umemployment_layoff(cur,conn)
    d = average_year_CPI(cur,conn)
    IR = avg_year_IR_Non_Marketable(cur,conn)


if __name__ == "__main__":
    main()