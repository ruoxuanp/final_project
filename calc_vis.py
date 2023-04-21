import sqlite3
import json
import os
import requests
import csv
import matplotlib
import matplotlib.pyplot as plt

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
    unemployment_l = list()
    layoff_l = list()
    lu_ratio_l = list()
    date_l = list()
    rows = cur.fetchall()
    for row in rows:
        inner = list()
        unemployment = row[0]
        date = row[1]
        layoff = row[2]
        inner.append(date)
        inner.append(round(layoff/unemployment,2))
        ratio.append(inner)
        unemployment_l.append(unemployment)
        layoff_l.append(layoff)
        lu_ratio_l.append(round(layoff/unemployment,2))
        date_l.append(date)

    csv_file = "ratio.csv"

    # Open the CSV file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

    # Write the numbers to the CSV file
        writer.writerow(["Date", "Ratio"])  # Write header row
        for num in ratio:
            writer.writerow(num)  # Write each number as a row

    return (layoff_l, unemployment_l, lu_ratio_l, date_l)
    


def avg_year_CPI(cur,conn):
    cur.execute('''SELECT cpi FROM Cpi_Rate''')
    rows = cur.fetchall()
    l = list()
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
        l.append(avg)
    
    csv_file = "year_average_CPI.csv"

    # Open the CSV file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

    # Write the numbers to the CSV file
        writer.writerow(["Year", "Average CPI"])  # Write header row
        for num in average:
            writer.writerow(num)  # Write each number as a row

    return(l)


def avg_year_IR_Non_Marketable(cur,conn):
    cur.execute('''
    SELECT avg_interest_rate_amt FROM IR_Non_Marketable 
    ''')
    rows_rate_Non_Marketable  = cur.fetchall()
    l = list()
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
        l.append(avg)
    
    csv_file = "year_average_IR.csv"

    # Open the CSV file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

    # Write the numbers to the CSV file
        writer.writerow(["Year", "Average Interest Rate"])  # Write header row
        for num in average:
            writer.writerow(num)  # Write each number as a row

    return(l)


def main():
    cur, conn = ConnectDatabase('umemployment_data.db')
    ratio = retrive_umemployment_layoff(cur,conn)
    cpi = avg_year_CPI(cur,conn)
    IR = avg_year_IR_Non_Marketable(cur,conn)
    yr = [*range(2013, 2023)]

    fig1 = plt.figure(figsize=(10,8))
    ax1 = fig1.add_subplot(111)
    ax1.plot(yr, cpi, marker = 'o')
    ax1.set_xlabel('year')
    ax1.set_ylabel('average CPI')
    ax1.set_title('Average yearly CPI 2013-2022')
    ax1.grid()

    fig1.savefig('avg_year_CPI.png')

    fig2 = plt.figure(figsize=(10,8))
    ax2 = fig2.add_subplot(111)
    ax2.plot(yr, IR, marker = 'o')
    ax2.set_xlabel('year')
    ax2.set_ylabel('average IR')
    ax2.set_title('Average yearly IR for Total Non-Marketable 2013-2022')
    ax2.grid()

    fig2.savefig('avg_year_IR_Non_Marketab.png')

    fig3 = plt.figure(figsize=(10,8))
    ax3 = fig3.add_subplot(111)
    ax3.scatter(ratio[1], ratio[0])
    ax3.set_xlabel('unemployment rate')
    ax3.set_ylabel('layoff rate')
    ax3.set_title('Layoff rate vs Unemployment rate 2013-2022')
    ax3.grid()

    fig3.savefig('layoff_vs_unemployment.png')

    fig4 = plt.figure(figsize=(10,8))
    ax4 = fig4.add_subplot(111)
    line1, =ax4.plot(ratio[3], ratio[0], label = 'Layoff rate')
    line2, =ax4.plot(ratio[3], ratio[1], label = 'Unemployment rate')
    line3, =ax4.plot(ratio[3], ratio[2], label = 'Layoff/Unemployment ratio')
    ax4.legend(handles=[line1, line2, line3])
    ax4.set_xlabel('date')
    ax4.set_ylabel('rates')
    ax4.set_title('Layoff, Unemployment rates & ratio 2013-2022')

    fig4.savefig('rates.png')
    
    plt.show()


if __name__ == "__main__":
    main()