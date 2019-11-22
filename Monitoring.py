import os, os.path, time, datetime
from datetime import datetime
import pandas as pd
from datetime import date


class Monitoring:
    def __init__(self, start_path, symbol, symbol_month, date):
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

    

M = Monitoring('/bigdata')

M.total_count()

M.total_size()

M.files_per_day()

M.files_per_day_per_directory()

M.files_creation()

M.files_modification()


