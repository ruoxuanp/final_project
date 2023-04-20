import sqlite3
import os
from readin.py import lay_off_data, unemployment_data, cpi_data, ir_marketable_data, ir_non_marketable_data


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

    # create IR_Marketable table in database
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

    # create IR_Non_Marketable table in database
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

    # create Unemployment table in database
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

    # create Cpi_Rate table in database
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
    