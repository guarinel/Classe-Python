import os, os.path, time, datetime
from datetime import datetime
import pandas as pd
from datetime import date
import time
import re

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

class Monitoring:
    def __init__(self, start_path):
        self.start_path = start_path
        
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








A = ['sddsds', 'nbbun', 'uni']

A(0)















M = Monitoring('/bigdata')

M.total_count()

M.total_size()

M.files_per_day()

M.files_per_day_per_directory()

M.files_creation()


A = M.files_modification()


A.head(10)

A['Data'] =A['File'].apply(lambda x: re.findall(r'\d{4}-\d{2}-\d{2}', x))

A['Data'] = ['-'.join(map(str, l)) for l in A['Data']]

A['Data'] = pd.to_datetime(A['Data'], errors = 'coerce', format = "%Y/%m/%d")


A =  A[( A['Data'] >= pd.Timestamp('2019-08-15')  )]

A.drop(['index', 'Date'], axis=1, inplace=True)

SYMBOL =("DOL" , "WDO")

TIPO = ('snapshot', 'incremental')

pattern_symbol = '|'.join([f'(?i){symbol}' for symbol in SYMBOL])

pattern_symbol

pattern_tipo = '|'.join([f'(?i){tipo}' for tipo in TIPO])        

A["Symbol"] = A['File'].str.contains(pattern_symbol)

A['Type'] = A['File'].str.contains(pattern_tipo)


A =  A [  ( A['Symbol'] == True )   ]

A =  A [  ( A['Type'] == True )   ]

A.File.count()

df['Counter'] = 1
A.groupby(['Data']).count()

A =  A[( (A['Data'] >= pd.Timestamp(date.today())  ))]

A =  A [ ( (A['Symbol'] == True)  & (A['Type'] == True)   ) ]

pd.Timestamp('2019-07-15')