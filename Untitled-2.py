import os, os.path, time, datetime
from datetime import datetime
import pandas as pd
from datetime import date
import time
import datetime as dt
import re


class Consultor:
    def __init__(self, start_path, symbol, tipo , initial_date = None, final_date= None):
        self.start_path = start_path
        self.symbol = symbol
        self.initial_date = initial_date
        self.final_date = final_date
        #self.symbol_month = symbol_month
        #self.initial_date = (date.today()-dt.timedelta(days=5*365)) if initial_date is None else initial_date
        #self.final_date = (date.today() if final_date is None else final_date
        self.tipo = tipo

    #Count all files in a given directory
    def _count_file(self, start_path=None):
        start_path = self.start_path if start_path is None else start_path
        file_count = sum(len(files) for _, _, files in os.walk(start_path))
        return file_count 

    #Lists all _subdirecory
    def _subdirecory(self):
        A = (list([x[0] for x in os.walk(self.start_path)]))
        return A

    #Count all files in a given directory including _subdirecory(discriminated)
    def total_count(self):
        for directory in self._subdirecory():
            print(directory, self._count_file(directory), 'Files')

    #Size(gb) of all files in a given directory
    def _get_size(self, start_path=None):
        start_path = self.start_path if start_path is None else start_path
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    #Size(gb) of all files in a given directory including _subdirecory(discriminated)
    def total_size(self):
        for directory in self._subdirecory():
            #if self._get_size(self.start_path)/10 ** 9 > 0.5:
            print(directory, self._get_size(directory)/10 ** 9, 'GygaBytes')

    #Number of files created in a day in a given directory including _subdirecory(NOT discriminated)  
    def files_per_day(self):
        A =[]
        B=[]        
        for directory in self._subdirecory():
            for file1 in os.listdir(directory):                
                a = datetime.strptime(time.ctime(os.path.getctime(directory +'/'+ file1)), '%a %b %d %H:%M:%S %Y')
                A.append(a)
                B.append(directory)                  
        C = (A,B)
        df = pd.DataFrame(C)
        df = df.T
        df = df.rename(columns = {0: 'Date', 1: 'Directory'}) 
        df['Date'] = (df['Date'].apply(lambda x : datetime.date(x)))
        df = df.groupby(['Date']).count().rename(columns={'Directory' : 'Count'})
        return df

    #Number of files created in a day in a given directory including _subdirecory(discriminated) 
    def files_per_day_per_directory(self):
        A =[]
        B=[]
        for directory in self._subdirecory():
            for file1 in os.listdir(directory):                
                a = datetime.strptime(time.ctime(os.path.getctime(directory +'/'+ file1)), '%a %b %d %H:%M:%S %Y')
                A.append(a)
                B.append(directory)                           
        C = (A,B)
        df = pd.DataFrame(C)
        df = df.T
        df = df.rename(columns = {0: 'Date', 1: 'Directory'}) 
        df['Date'] = (df['Date'].apply(lambda x : datetime.date(x)))
        df['Counter'] = 1
        df = df.groupby(['Date','Directory'])['Counter'].sum()
        return df


    # List of all files per creation date 
    def files_creation(self): 
        C = []
        D = []
        for directory in self._subdirecory():
            for file1 in os.listdir(directory):
                c = datetime.strptime(time.ctime(os.path.getctime(directory +'/'+ file1)), '%a %b %d %H:%M:%S %Y')
                C.append(file1)
                D.append(c)    
        df = pd.DataFrame(C,D)
        df = df.reset_index()    
        df = df.rename(columns = {'index': 'Date', 0: 'File'}) 
        df['Date'] = (df['Date'].apply(lambda x : datetime.date(x)))
        return df

    # List of all files per last modification date 
    def files_modification(self): 
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

    ################################################################3

    def _cod_filter(self):
        symbol = self.symbol
        if symbol == "All":
            return self.files_modification()
        else:
            A = self.files_modification()
            A.drop(['index', 'Date'], axis=1, inplace=True)
            pattern_symbol = '|'.join([f'(?i){ticker}' for ticker in self.symbol])
            A["Symbol"] = A['File'].str.contains(pattern_symbol)
            A =  A [  ( A['Symbol'] == True )   ]
            return A

    def _type_filter(self, tipo = None):
        tipo = self.tipo
        if tipo is None:
            return self._cod_filter()
        else:
            A = self._cod_filter()
            pattern_tipo = '|'.join([f'(?i){modo}' for modo in tipo])        
            A['Type'] = A['File'].str.contains(pattern_tipo)
            A =  A [  ( A['Type'] == True )   ]
            return A

    def _date_filter(self, initial_date = None, final_date = None):
        initial_date = (date.today()-dt.timedelta(days=5*365)) if self.initial_date is None else self.initial_date
        final_date = (date.today()) if self.final_date is None else self.final_date
        A = self.files_modification()
        A['Data'] =A['File'].apply(lambda x: re.findall(r'\d{4}-\d{2}-\d{2}', x))
        #A['Data'] = ['-'.join(map(str, l)) for l in A['Data']]
        #A['Data'] = pd.to_datetime(A['Data'], errors = 'coerce', format = "%Y/%m/%d")
        #A =  A[( A['Data'] >= pd.Timestamp(initial_date)  )]
        #A =  A[( A['Data'] <= pd.Timestamp(final_date)  )]
        #DATA = list(A['File'])
                
        return A

    
    def count_files(self):
        A = self._date_filter()
        print('Total de arquivos e: ', A.File.count())


    def list_files(self):
        A = self._date_filter()
        return A
    






M._date_filter()



M = Consultor('/bigdata', 'DOLZ', 'snapshot', '2019-08-15', '2019-10-18' )


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