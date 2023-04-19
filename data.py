import unittest
import sqlite3
import json
import os
import pandas as pd

def read_econdb(api):
    # read in data from EconDB
    df = pd.read_csv(
    'https://www.econdb.com/api/series/JLRUS/?token=%s&format=csv' % api,
    index_col='Date', parse_dates=['Date'])
    return df


def main():
    #API for EconDB
    API = '46c24fbb4887bf33205204f709392879bdae6177'
    df = read_econdb(API)
    print(df)
    

if __name__ == "__main__":
    main()
    # You can comment this out to test with just the main function,
    # But be sure to uncomment it and test that you pass the unittests before you submit!
    
