import os, os.path
import datetime as dt
import pandas as pd
import re
from datetime import datetime
from datetime import date
from os import listdir
from os.path import isfile, join

class Consultor:
    def __init__(self):
        pass
       
    def _get_parameters(self, start_path, initial_date = None, final_date = None):
        all_files = listdir(start_path)
        df = pd.DataFrame([f for f in all_files if f.endswith(".parquet")])
        df = df.rename(columns = {0: 'File'})  
        
        if (initial_date != None or final_date != None):     
            df['Date'] =df['File'].apply(lambda x: re.findall(r'\d{4}-\d{2}-\d{2}', x))
            df['Date'] = ['-'.join(map(str, l)) for l in df['Date']]
            df['Date'] = pd.to_datetime(df['Date'], errors = 'coerce', format = "%Y/%m/%d")
        
        if initial_date != None:

            df =  df[(df['Date'] >= pd.Timestamp(initial_date))]
        
        if final_date != None:
            df =  df[(df['Date'] <= pd.Timestamp(final_date))]
        DATA = list(df['File'])
        return DATA

    # @profile
    def get_events(self, initial_date=None, final_date=None, symbol=None, type_=None, Name_ = None):
        start_path = os.getenv("HOME") + '/bigdata/events' if os.path.isdir(os.getenv("HOME") + '/bigdata') else '/bigdata/events'
        if Name_ is None:
            NAME = self._get_parameters(initial_date= initial_date, final_date=final_date, start_path= start_path)
        else: 
            NAME = Name_
        dict_info= {}  
        pattern_type_ = type_               
        pattern_symbol = symbol
        
        if type_ is None:
            pattern_type_ = ['snapshot', 'incremental']
        elif type_ is not None:
            if type(pattern_type_) is str:
                pattern_type_ = [pattern_type_]
            elif type(pattern_type_) is tuple:
                pattern_type_ = list(pattern_type_)

        #User enters parameters only for Inc 
        if symbol is None:
            for title in NAME:
                split = title.split('-')
                symb = split[0]
                if symb not in dict_info:        
                    dict_info[symb] = {}                
                for inc in pattern_type_:
                    if inc not in dict_info[symb]:
                        dict_info[symb][inc] = []                        
                    if symb in title:
                        if inc in title:
                            date_ = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                            date_ = '-'.join(date_)     
                            dict_info[symb][inc] += [date_]

         # User enters parameters for SYMBOL & INCREMENTAL
        elif symbol is not None:                       
            if type(pattern_symbol) is str:
                pattern_symbol = [pattern_symbol]
            elif type(pattern_symbol) is tuple:
                pattern_symbol = list(pattern_symbol)                 
            for symb_ in pattern_symbol:
                for title in NAME:                    
                    if symb_ in title:
                        split = title.split('-')
                        symb = split[0]
                        if symb not in dict_info:        
                            dict_info[symb] = {}                
                        for inc in pattern_type_:
                            if inc not in dict_info[symb]:
                                dict_info[symb][inc] = []                        
                            if symb in title:
                                if inc in title:
                                    date_ = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date_ = '-'.join(date_)     
                                    dict_info[symb][inc] += [date_]
     
        return  dict_info

    def _load_files(self, location):
        start_path = os.getenv("HOME") + '/bigdata/database' if os.path.isdir(os.getenv("HOME") + '/bigdata') else '/bigdata/database'
        os.chdir(start_path)
        df_historical = pd.read_csv("allfiles.csv", sep = ',')
        df_trades = pd.read_csv("alltrades.csv", sep = ';')
        
        if location == 'events': 
            return list(df_historical['title'])
        elif location == 'trades':
            return list(df_trades['title'])

    def diff_events(self, initial_date=None, final_date=None, symbol=None, type_=None):
        events = self.get_events(initial_date=initial_date, final_date=final_date, symbol=symbol, type_=type_, Name_ = None)
        df = self._load_files(location= 'events')        
        historical = self.get_events(initial_date=initial_date, final_date=final_date, symbol=symbol, type_=type_, Name_ = df )
        dict_final ={}

        for key in historical:
            if key not in events:
                dict_final[key] = historical[key]
                events[key] = {'snapshot':[], 'incremental':[]}              
            dict_final[key]={}                      
            for key2 in historical[key]:
                set_historical = set(historical[key][key2])  
                set_events = set(events[key][key2])
                diff = list(set_historical.difference(set_events))
                dict_final[key][key2] = diff

        return dict_final

    def get_trades(self, initial_date = None, final_date = None, symbol = None, Name_ = None):
        start_path = os.getenv("HOME") + '/bigdata/trades' if os.path.isdir(os.getenv("HOME") + '/bigdata') else '/bigdata/trades'
        if Name_ is None:
            NAME = self._get_parameters(initial_date= initial_date, final_date=final_date, start_path= start_path)
        else: 
            NAME = Name_     
        dict_info= {}                       
        pattern_symbol = symbol

        if symbol is not None:
            if type(pattern_symbol) is str:
                pattern_symbol = [pattern_symbol]
            elif type(pattern_symbol) is tuple:
                pattern_symbol = list(pattern_symbol)
            for symb_ in pattern_symbol:
                for title in NAME:
                    if symb_ in title:
                        split = title.split('-')
                        symb = split[0]
                        if symb not in dict_info:        
                            dict_info[symb] = []
                        if symb in title:
                            date_ = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                            date_ = '-'.join(date_)                                     
                            dict_info[symb] += [date_]           
        elif symbol is None:            
            for title in NAME:
                split = title.split('-')
                symb = split[0]
                if symb not in dict_info:     
                    dict_info[symb] = []                                   
                if symb in title:                            
                    date_ = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                    date_ = '-'.join(date_)                                     
                    dict_info[symb] += [date_]

        return dict_info

    def diff_trades(self, initial_date=None, final_date=None, symbol=None):
        events = self.get_trades(initial_date=initial_date, final_date=final_date, symbol=symbol, Name_ = None)
        df = self._load_files(location= 'trades')        
        historical = self.get_trades(initial_date=initial_date, final_date=final_date, symbol=symbol, Name_ = df )
        dict_final ={}

        for key in historical:
            if key not in events:
                dict_final[key] = historical[key]
                events[key]=[]
            set_historical = set(historical[key])  
            set_events = set(events[key])
            diff = list(set_historical.difference(set_events))
            dict_final[key]= diff

        return dict_final

    def get_order_book(self, initial_date = None, final_date = None, symbol = None):
        start_path = os.getenv("HOME") + '/bigdata/order-book' if os.path.isdir(os.getenv("HOME") + '/bigdata') else '/bigdata/order-book'                
        NAME = self._get_parameters(initial_date= initial_date, final_date=final_date, start_path= start_path)
        dict_info= {}                       
        pattern_symbol = symbol
        field = ('broker', 'inc_code', 'order_id', 'price', 'quantity')
        side = ('bid', 'ask')

        if symbol is not None:
            if type(pattern_symbol) is str:
                pattern_symbol = [pattern_symbol]
            elif type(pattern_symbol) is tuple:
                pattern_symbol = list(pattern_symbol)
        elif symbol is None:
            for title in NAME:
                split = title.split('-')
                pattern_symbol = split[0]           
                pattern_symbol = [pattern_symbol]  
        
        for symb in pattern_symbol:
            dict_info[symb] = {}                             
            for field_ in field:
                dict_info[symb][field_] = {}
                for side_ in side:
                    dict_info[symb][field_][side_] = []
                    for title in NAME:
                        if symb in title:
                            if field_ in title:
                                if side_ in title:
                                    date_ = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date_ = '-'.join(date_)                                     
                                    dict_info[symb][field_][side_] += [date_]       
        return dict_info

    def get_level_book(self, initial_date = None, final_date = None, symbol = None):
        start_path = os.getenv("HOME") + '/bigdata/level-book' if os.path.isdir(os.getenv("HOME") + '/bigdata') else '/bigdata/level-book'
        NAME = self._get_parameters(initial_date= initial_date, final_date=final_date, start_path= start_path)
        dict_info= {}                       
        pattern_symbol = symbol
        field = ('broker', 'price', 'quantity')
        side = ('bid', 'ask')

        if symbol is not None:
            if type(pattern_symbol) is str:
                pattern_symbol = [pattern_symbol]
            elif type(pattern_symbol) is tuple:
                pattern_symbol = list(pattern_symbol)
        elif symbol is None:
            for title in NAME:
                split = title.split('-')
                pattern_symbol = split[0]           
                pattern_symbol = [pattern_symbol]  
        for symb in pattern_symbol:
            dict_info[symb] = {}                             
            for field_ in field:
                dict_info[symb][field_] = {}
                for side_ in side:
                    dict_info[symb][field_][side_] = []
                    for title in NAME:
                        if symb in title:
                            if field_ in title:
                                if side_ in title:
                                    date_ = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date_ = '-'.join(date_)                                     
                                    dict_info[symb][field_][side_] += [date_]
        
        return dict_info


if __name__ == '__main__':
    from asimov_database import Consultor
    symbol_stock = 'MRVE3'

    symbol_future = 'DOL'
    inc = 'incremental'

    initial_date = '2019-07-15'
    final_date = '2019-11-18'

    consult = Consultor()     

    consult.get_events()
    consult.diff_events()
    consult.diff_trades()
    consult.get_trades(initial_date=initial_date, final_date=final_date, symbol= symbol_future)
    consult.get_order_book(initial_date=initial_date, final_date=final_date, symbol=symbol_stock)
    consult.get_level_book(initial_date=initial_date, final_date=final_date, symbol=symbol_stock)
