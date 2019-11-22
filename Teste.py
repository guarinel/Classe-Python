import os, os.path, time, datetime
from datetime import datetime
import pandas as pd
from datetime import date
import time
import datetime as dt
import re


class Consultor:
    def __init__(self, start_path, symbol , tipo , initial_date = None, final_date= None):
        self.start_path = start_path
        self.initial_date = initial_date
        self.symbol = symbol
        self.final_date = final_date       
        self.tipo = tipo

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

   
    def _date_filter(self, initial_date = None, final_date = None):
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
    
    def _cod_filter(self):        
        dict_info= {}                
        DATA = self._date_filter()
        pattern_tipo = self.tipo
        NAME = self._date_filter()
        
        # User enters parameters only for INCREMENTAL
        if self.symbol == "All" and self.tipo != 'All':
            for symb in NAME:
                split = symb.split('-')
                symb = split[0]
                dict_info[symb] = {}
                for inc in pattern_tipo:
                        dict_info[symb][inc] = []                        
                        for title in NAME:                            
                            if symb in title:
                                if inc in title:
                                    date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                    date = '-'.join(date)                                     
                                    dict_info[symb][inc] += [date]  

        # User enters parameters for SYMBOL & INCREMENTAL
        elif self.symbol != "All" and self.tipo != 'All':
            NAME = self._date_filter()
            pattern_symbol = self.symbol            
            
            #Checks if is string or tuple for python method interation
            if type(pattern_symbol) is tuple:  
                
                for symb in pattern_symbol:
                    dict_info[symb] = {}                   
                    for inc in pattern_tipo:
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
                for inc in pattern_tipo:
                    dict_info[symb][inc] = []                        
                    for title in NAME:                            
                        if symb in title:
                            if inc in title:
                                date = re.findall(r'\d{4}-\d{2}-\d{2}', title)
                                date = '-'.join(date)                                     
                                dict_info[symb][inc] += [date]     

        

        # User enters parameters only  for SYMBOL
        elif self.symbol != "All" and self.tipo == 'All':  
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



M._cod_filter()

for keys in A.keys():
    print(keys)

M._date_filter

print(A)

M = Consultor('/bigdata', 'DOLQ', ('snapshot', 'incremental'))




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

#TIPO = ('snapshot', 'incremental')

pattern_symbol = '|'.join([f'(?i){ticker}' for ticker in self.symbol])

A["Symbol"] = A['File'].str.contains(pattern_symbol)

pattern_tipo = '|'.join([f'(?i){modo}' for modo in self.tipo])        


A['Type'] = A['File'].str.contains(pattern_tipo)

A =  A[( ( pd.Timestamp(self.data_inicial) >= A['Data'] >= pd.Timestamp(self.data_final) ))]

A =  A [ ( (A['Symbol'] == True)  & (A['Type'] == True)   ) ]

        
type(a)
type(today)

    def acesso(self, start_path = None, symbol= None, symbol_month= None, tipo = None, initial_date = None, final_date= None):
        symbol_month = self.symbol_month if symbol_month is None else symbol_month
        start_path = self.start_path if start_path is None else start_path
        symbol = self.symbol if symbol is None else symbol
        initial_date = self.initial_date if initial_date is None else (date.today()-dt.timedelta(days=5*365))
        final_date = self.final_date if final_date is None else (date.today())
        tipo = self.tipo if tipo is None else tipo

        
        
        
        A = self.files_modification()

        SYMBOL = self.symbol

        TIPO = self.tipo
A.head()


type('nmikmo')

C = ('bhb','nnjkn', 'bnjun')

type(C)


list('tetse')