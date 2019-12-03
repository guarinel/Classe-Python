import psycopg2
import pandas as pd
import os
import numpy as np
from datetime import date, datetime, timedelta
import time

class Uptader:
    def __init__(self):
        self.conn = None
        self.valid_database = None

    def _create_sql_connection(self, database='md_rt'):
        check = 0
        while check == 0 and self.conn is None and self.valid_database != database:
            self.valid_database = database
            try:
                params = {
                    'database': database,
                    'user': 'asimov',
                    'password': 'asimov',
                    'host': '10.112.1.20',
                    'port': 5432
                    }
                self.conn = psycopg2.connect(**params)
                check = 1
            except:
                print(' [ PARQUET ] Error on connection creation. Trying again...')
                time.sleep(5)

    def _load_postgres_data_for_incremental(self, db='md_rt'):
        self._create_sql_connection(db)
        today = date.today()# - timedelta(days=191)).strftime("%Y-%m-%d %H:%M:%S"))
        yesterday = ((date.today() - timedelta(days=9)).strftime("%Y-%m-%d %H:%M:%S")) 
        symbol = ('WDOZ19','WDOQ19', 'DOLU19', 'DOLV19', 'DOLN19', 'DOLQ19', 'WDOV19', 'DOLK19', 'WDON19', 'DOLH19', 'WDOJ19', 'WDOX19', 'DOLJ19', 'WDOM19', 'DOLM19', 'WDOK19', 'DOLZ19', 'WDOU19', 'WDOF19', 'WDOG19', 'WDOH19', 'DOLF19', 'DOLG19', 'DOLX19')   
        
        string_inc = "SELECT distinct symbol, EXTRACT(month FROM ts)*100 + EXTRACT(day FROM ts) as month_ FROM {} WHERE ts between '{}' AND '{}' AND symbol in {}".format('md_incremental', yesterday, today, symbol)   
        df_incremental = pd.read_sql_query(string_inc, self.conn)
        df_incremental['inc'] = 'incremental'
        
        string_snap = "SELECT distinct symbol, EXTRACT(month FROM ts)*100 + EXTRACT(day FROM ts) as month_ FROM {} WHERE ts between '{}' AND '{}' AND symbol in {}".format('md_snapshot', yesterday, today, symbol)
        df_snapshot = pd.read_sql_query(string_snap, self.conn)
        df_snapshot['inc'] = 'snapshot'

        df_total = pd.concat([df_incremental, df_snapshot])

        return df_total

    def _load_postgres_data_for_trades(self, db='md_rt'):    
        self._create_sql_connection(db)
        today = date.today()# - timedelta(days=191)).strftime("%Y-%m-%d %H:%M:%S"))
        yesterday = ((date.today() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")) 
        symbol = ('WDOZ19','WDOQ19', 'DOLU19', 'DOLV19', 'DOLN19', 'DOLQ19', 'WDOV19', 'DOLK19', 'WDON19', 'DOLH19', 'WDOJ19', 'WDOX19', 'DOLJ19', 'WDOM19', 'DOLM19', 'WDOK19', 'DOLZ19', 'WDOU19', 'WDOF19', 'WDOG19', 'WDOH19', 'DOLF19', 'DOLG19', 'DOLX19')   

        string_trade = "SELECT distinct symbol, EXTRACT(month FROM ts)*100 + EXTRACT(day FROM ts) as month_ FROM {} WHERE ts between '{}' AND '{}' AND symbol in {}".format('md_trade', yesterday, today, symbol)
        df_trade = pd.read_sql_query(string_trade, self.conn)
       
        df_trade['Day'] = df_trade.month_ % 100
        df_trade['Month'] = (df_trade.month_ // 100)
        df_trade['Year'] = '2019'
        df_trade['Month'] =df_trade['Month'].astype(int)
        df_trade['Day'] =df_trade['Day'].astype(int)
        df_trade['Month'] =df_trade['Month'].astype(str)
        df_trade['Day'] =df_trade['Day'].astype(str)
        df_trade['Day'] = df_trade['Day'].apply(lambda x: '{0:0>2}'.format(x))
        df_trade['Month'] = df_trade['Month'].apply(lambda x: '{0:0>2}'.format(x))
        df_trade['title'] = df_trade.symbol + '-'  + df_trade.Year + '-'+ df_trade.Month + '-' +  df_trade.Day
        df_trade  = df_trade.drop(columns=['Day', 'Month', 'symbol', 'month_', 'Year'])


        return df_trade

    def _cleanser_for_inc(self):     

        df_final = self._load_postgres_data_for_incremental()

        df_final['Day'] = df_final.month_ % 100
        df_final['Month'] = (df_final.month_ // 100)
        df_final['Year'] = '2019'
        df_final['Month'] =df_final['Month'].astype(int)
        df_final['Day'] =df_final['Day'].astype(int)
        df_final['Month'] =df_final['Month'].astype(str)
        df_final['Day'] =df_final['Day'].astype(str)
        df_final['Day'] = df_final['Day'].apply(lambda x: '{0:0>2}'.format(x))
        df_final['Month'] = df_final['Month'].apply(lambda x: '{0:0>2}'.format(x))
        df_final['title'] = df_final.symbol + '-' + df_final.inc + '-' + df_final.Year + '-'+ df_final.Month + '-' +  df_final.Day
        df_final  = df_final.drop(columns=['Day', 'Month', 'symbol', 'month_', 'inc', 'Year'])
    
        return df_final

    def append_to_file(self):
        start_path = os.getenv("HOME") + '/bigdata/database' if os.path.isdir(os.getenv("HOME") + '/bigdata') else '/bigdata/database'
        os.chdir(start_path)
        
        df_historical_inc = pd.read_csv("allfiles.csv", sep = ',')
        df_historical_trade = pd.read_csv("alltrades.csv", sep = ',')

        df_new_trade = self._load_postgres_data_for_trades()
        df_new_inc  = self._cleanser_for_inc()

        df_uptaded_trade = pd.concate(df_historical_trade, df_new_trade)
        df_uptaded_inc = pd.concate(df_historical_inc, df_new_inc)

        df_uptaded_trade.drop_duplicates(subset ="title", keep = 'first', inplace = True)
        df_uptaded_inc.drop_duplicates(subset ="title", keep = 'first', inplace = True)

        df_uptaded_inc.to_csv(start_path + 'allfiles.csv', sep = ',', header = True, index = False)
        df_uptaded_trade.to_csv(start_path + 'alltrades.csv', sep = ',', header = True, index = False)


if __name__ == '__main__':
    from asimov_database import Uptader
    uptade = Uptader()

    uptade._cleanser_for_inc()

