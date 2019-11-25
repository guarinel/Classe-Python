import os, os.path, time, datetime
from datetime import datetime
import pandas as pd
from datetime import date
import time
import datetime as dt
import re


class Consultor:
    def __init__(self):
        self.teste = 16       

    def get_directory(self, start_path):
        self.start_path = '/bigdata' + start_path

    #Lists all _subdirecory
    def _subdirecory(self):
        A = (list([x[0] for x in os.walk(self.start_path)]))
        return A

      
    # List of all files per last modification date 
    def _files_modification(self): 
        C = []
        D = []
        for directory in self._subdirecory():
            for file1 in os.listdir(directory):
                c = datetime.strptime(time.ctime(os.path.getmtime(directory +'/'+ file1)), '%a %b %d %H:%M:%S %Y')
                if '.parquet' in file1:
                    C.append(file1)
                    D.append(c)   
        df = pd.DataFrame(C,D)
        df = df.reset_index()    
        df = df.rename(columns = {'index': 'Date', 0: 'File'}) 
        df['Date'] = (df['Date'].apply(lambda x : datetime.date(x)))
        df = df [df['File'].str.contains('.parquet')]
        df = df.dropna()
        df = df.reset_index()
        
        return df 
    
    def get_date(self, inital_date = None, final_date = None):
        self.initial_date = inital_date
        self.final_date = final_date

        
    def _date_filter(self):
        initial_date = (date.today()-dt.timedelta(days=5*365)) if self.initial_date is None else self.initial_date
        final_date = (date.today()) if self.final_date is None else self.final_date
        A = self._files_modification()
        A['Data'] =A['File'].apply(lambda x: re.findall(r'\d{4}-\d{2}-\d{2}', x))
        A['Data'] = ['-'.join(map(str, l)) for l in A['Data']]
        A['Data'] = pd.to_datetime(A['Data'], errors = 'coerce', format = "%Y/%m/%d")
        A =  A[( A['Data'] >= pd.Timestamp(initial_date)  )]
        A =  A[( A['Data'] <= pd.Timestamp(final_date)  )]
        DATA = list(A['File'])
                
        return DATA
    

    def get_events(self, symbol, type_):
        dict_info= {}                
        #DATA = self._date_filter()
        pattern_type_ = self.type_
        pattern_symbol = self.symbol
        NAME = self._date_filter()

        if (self.symbol == "All" and self.type_ != 'All'):
            for symb in NAME:
                split = symb.split('-')
                symb = split[0]
                dict_info[symb] = {}
                if type(pattern_type_) is tuple: 
                    for inc in pattern_type_:
                            dict_info[symb][inc] = []                        
                            for title in NAME:                            
                                if symb in title:
                                    if inc in title:
                                        date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                        date = '-'.join(date)                                     
                                        dict_info[symb][inc] += [date]  

                elif type(pattern_type_) is str:
                    inc = pattern_type_
                    dict_info[symb][inc] = []                        
                    for title in NAME:                            
                        if symb in title:
                            if inc in title:
                                date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                date = '-'.join(date)                                     
                                dict_info[symb][inc] += [date]  
            
        # User enters parameters for SYMBOL & INCREMENTAL
        elif (self.symbol != "All" and self.type_ != 'All'):
                       
            #Checks if is string or tuple for python method of iteration
            if (type(pattern_symbol) is tuple and type(pattern_type_) is tuple):  
                
                for symb in pattern_symbol:
                    dict_info[symb] = {}                   
                    for inc in pattern_type_:
                        dict_info[symb][inc] = []                        
                        for title in NAME:                            
                            if symb in title:
                                if inc in title:
                                    date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date = '-'.join(date)                                     
                                    dict_info[symb][inc] += [date]  

            elif (type(pattern_symbol) is str and type(pattern_type_) is str):

                symb = pattern_symbol
                dict_info[symb] = {}                   
                inc = pattern_type_
                dict_info[symb][inc] = []                        
                for title in NAME:                            
                    if symb in title:
                        if inc in title:
                            date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                            date = '-'.join(date)                                     
                            dict_info[symb][inc] += [date] 
    
            elif (type(pattern_symbol) is str and type(pattern_type_) is tuple):  
                
                symb = pattern_symbol
                dict_info[symb] = {}                   
                for inc in pattern_type_:
                    dict_info[symb][inc] = []                        
                    for title in NAME:                            
                        if symb in title:
                            if inc in title:
                                date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                date = '-'.join(date)                                     
                                dict_info[symb][inc] += [date]

            elif (type(pattern_symbol) is tuple and type(pattern_type_) is str):  
                
                inc = pattern_type_
                for symb in pattern_symbol:
                    dict_info[symb] = {}                   
                    dict_info[symb][inc] = []                        
                    for title in NAME:                            
                        if symb in title:
                            if inc in title:
                                date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                date = '-'.join(date)                                     
                                dict_info[symb][inc] += [date]

        #################No parameters
        elif (self.symbol == "All" and self.type_ == 'All'):   

            pattern_type_ = ('snapshot', 'incremental')
            for symb in NAME:
                split = symb.split('-')
                symb = split[0]
                dict_info[symb] = {}
                for inc in pattern_type_:
                        dict_info[symb][inc] = []                        
                        for title in NAME:                            
                            if symb in title:
                                if inc in title:
                                    date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date = '-'.join(date)                                     
                                    dict_info[symb][inc] += [date]  

        ######################        
        elif (self.symbol != "All" and self.type_ == 'All'):
            
            pattern_type_ = ('snapshot', 'incremental') 
            if (type(pattern_symbol) is tuple):
                for symb in pattern_symbol:
                        dict_info[symb] = {}                   
                        for inc in pattern_type_:
                            dict_info[symb][inc] = []                        
                            for title in NAME:                            
                                if symb in title:
                                    if inc in title:
                                        date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                        date = '-'.join(date)                                     
                                        dict_info[symb][inc] += [date]

            elif (type(pattern_symbol) is str): 
                symb = pattern_symbol
                dict_info[symb] = {}                   
                for inc in pattern_type_:
                    dict_info[symb][inc] = []                        
                    for title in NAME:                            
                        if symb in title:
                            if inc in title:
                                date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                date = '-'.join(date)                                     
                                dict_info[symb][inc] += [date]

        return dict_info



    def get_trades(self, symbol):
        self.symbol = symbol
        
        dict_info= {}                
        pattern_symbol = self.symbol
        NAME = self._date_filter()

        if self.symbol != "All":
            if type(pattern_symbol) is tuple:                
                for symb in pattern_symbol:
                    dict_info[symb] = []                                         
                    for title in NAME:                            
                        if symb in title:
                            date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                            date = '-'.join(date)                                     
                            dict_info[symb] += [date]  

            elif type(pattern_symbol) is str:
                symb = pattern_symbol
                dict_info[symb] = []                           
                for title in NAME:                            
                    if symb in title:                        
                        date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                        date = '-'.join(date)                                     
                        dict_info[symb] += [date] 
    
           ##########################################

        elif self.symbol == "All":            
            for symb in NAME:
                split = symb.split('-')
                symb = split[0]
                dict_info[symb] = []                                   
                for title in NAME:                            
                    if symb in title:                            
                        date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                        date = '-'.join(date)                                     
                        dict_info[symb][inc] += [date]

        return dict_info



     
    def _cod_filter(self, symbol, type_) :        
        self.symbol = symbol
        self.type_ = type_

        dict_info= {}                
        DATA = self._date_filter()
        pattern_type_ = self.type_
        NAME = self._date_filter()
        
        # User enters parameters only for INCREMENTAL
        if self.symbol == "All" and self.type_ != 'All':
            for symb in NAME:
                split = symb.split('-')
                symb = split[0]
                dict_info[symb] = {}
                for inc in pattern_type_:
                        dict_info[symb][inc] = []                        
                        for title in NAME:                            
                            if symb in title:
                                if inc in title:
                                    date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date = '-'.join(date)                                     
                                    dict_info[symb][inc] += [date]  

        # User enters parameters for SYMBOL & INCREMENTAL
        elif self.symbol != "All" and self.type_ != 'All':
            NAME = self._date_filter()
            pattern_symbol = self.symbol            
            
            #Checks if is string or tuple for python method interation
            if type(pattern_symbol) is tuple:  
                
                for symb in pattern_symbol:
                    dict_info[symb] = {}                   
                    for inc in pattern_type_:
                        dict_info[symb][inc] = []                        
                        for title in NAME:                            
                            if symb in title:
                                if inc in title:
                                    date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date = '-'.join(date)                                     
                                    dict_info[symb][inc] += [date]       

            #Checks if is string or tuple for python method interation
            elif type(pattern_symbol) is str:

                symb = pattern_symbol
                dict_info[symb] = {}                   
                for inc in pattern_type_:
                    dict_info[symb][inc] = []                        
                    for title in NAME:                            
                        if symb in title:
                            if inc in title:
                                date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                date = '-'.join(date)                                     
                                dict_info[symb][inc] += [date]     

        

        # User enters parameters only  for SYMBOL
        elif self.symbol != "All" and self.type_ == 'All':  
            for symb in pattern_symbol:
                dict_info[symb] = {}                            
                for title in NAME:                            
                    if symb in title:
                        if inc in title:
                            date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                            date = '-'.join(date)                                     
                            dict_info[symb] += [date]                                    
        
              

        return dict_info




M._files_modification()

os.getcwd()

M._cod_filter()

for keys in A.keys():
    print(keys)

M._date_filter

print(A)

M = Consultor('C:\\Users\\guari\\Desktop\\database-master\\asimov_database\\database', 'DOLQ', ('snapshot', 'incremental'))




list_files = M._date_filter()
list_files = [i for i in list_files if "incremental" in i or "snapshot" in i]


dict_1 = {}
for file_ in list_files:
    split = file_.split('-')
    symb = split[0]
    inc = split[1]
    dates = split[2] + "-" + split[3] + "-" +  split[4].split(".")[0]

    if symb not in dict_1:
        dict_1[symb] = {}

    if inc not in dict_1[symb]:
        dict_1[symb][inc] = []
    dict_1[symb][inc] += [dates]

dict_1["WDOH19"].keys()

A['DOLM19'].keys()

A.head()


M.list_files()

A.drop(['index', 'Date'], axis=1, inplace=True)

A.drop(['index'], axis=1, inplace=True)


A = A.reset_index()

A = M.count_files()


A.to_dict('dict')

print(A.to_dict('dict'))




A['Data'] =A['File'].apply(lambda x: re.findall(r'\d{4}-\d{2}-\d{2}', x))


A['Data'] = ['-'.join(map(str, l)) for l in A['Data']]

A['Data'] = pd.to_datetime(A['Data'], errors = 'coerce', format = "%Y/%m/%d")


#SYMBOL =("DOL" , "WDO")

#type_ = ('snapshot', 'incremental')

pattern_symbol = '|'.join([f'(?i){ticker}' for ticker in self.symbol])

A["Symbol"] = A['File'].str.contains(pattern_symbol)

pattern_type_ = '|'.join([f'(?i){modo}' for modo in self.type_])        


A['Type'] = A['File'].str.contains(pattern_type_)

A =  A[( ( pd.Timestamp(self.data_inicial) >= A['Data'] >= pd.Timestamp(self.data_final) ))]

A =  A [ ( (A['Symbol'] == True)  & (A['Type'] == True)   ) ]

        
type(a)
type(today)

def acesso(self, start_path = None, symbol= None, symbol_month= None, type_ = None, initial_date = None, final_date= None):
    symbol_month = self.symbol_month if symbol_month is None else symbol_month
    start_path = self.start_path if start_path is None else start_path
    symbol = self.symbol if symbol is None else symbol
    initial_date = self.initial_date if initial_date is None else (date.today()-dt.timedelta(days=5*365))
    final_date = self.final_date if final_date is None else (date.today())
    type_ = self.type_ if type_ is None else type_

        
        

A = self.files_modification()

SYMBOL = self.symbol

type_ = self.type_
A.head()


type('nmikmo')

C = ('bhb','nnjkn', 'bnjun')

type(C)


list('tetse')