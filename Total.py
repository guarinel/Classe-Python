import os, os.path, time, datetime
from datetime import datetime
import pandas as pd
from datetime import date
#Count of files in directory including subdirectories

def count_file(start_path):
    file_count = sum(len(files) for _, _, files in os.walk(start_path))
    return file_count 

def subdirectories(pasta):
    A = (list([x[0] for x in os.walk(pasta)]))
    return A

for directory in subdirectories(directory2):
        print(directory, count_file(directory), 'Files')


#Size of direcory including subdirectories

import os, os.path

def get_size(start_path):
    total_size = 0
    A = []
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            A.append(f)
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    print (len(A))
    return total_size

get_size(directory2)

len(A)

def subdirectories(pasta):
    A = (list([x[0] for x in os.walk(pasta)]))
    return A

directory2 = '/bigdata'

subdirectories(directory2)

for directory in subdirectories(directory2):
    if get_size(directory2)/10 ** 9 > 0.5:
        print(directory, get_size(directory)/10 ** 9, 'GygaBytes')


# Time Monitoring

directory2 = '/bigdata'

def subdirectories(pasta):
    A = (list([x[0] for x in os.walk(pasta)]))
    return A


def time_monitoring(P_directory):
    A =[]
    B=[]
    #dt = datetime.datetime.today()
    for directory in subdirectories(P_directory):
        for file1 in os.listdir(directory):                
            a = datetime.strptime(time.ctime(os.path.getmtime(directory +'/'+ file1)), '%a %b %d %H:%M:%S %Y')
            A.append(a)
            B.append(directory)           
            #b = datetime.strptime(time.ctime(os.path.getmtime(directory +'/'+ file1)), '%a %b %d %H:%M:%S %Y')
            #B.append(b, directory)
    C = (A,B)
    df = pd.DataFrame(C)
    df = df.T
    df = df.rename(columns = {0: 'Date', 1: 'Directory'}) 
    df['Date'] = (df['Date'].apply(lambda x : datetime.date(x)))
    #df['Counter'] = 1
    #df = df.groupby(['Date','Directory'])['Counter'].sum()
    #df = df.groupby(['Date', 'Directory']).count() #.rename(columns={'Directory' : 'Count'})
    #df = df.groupby(['Date','Directory']).size().reset_index().groupby('Directory')[[0]].max()
    #print (df)
    return df
            #print(directory + ": " + file1)
            #print("last modified: %s" % time.ctime(os.path.getmtime(directory +'/'+ file1)))
            #print("created: %s" % time.ctime(os.path.getctime(directory +'/'+ file1)))



def list_of_files(P_directory):
    #total_size = 0
    C = []
    D = []
    for directory in subdirectories(P_directory):
        for file1 in os.listdir(directory):
            c = datetime.strptime(time.ctime(os.path.getmtime(directory +'/'+ file1)), '%a %b %d %H:%M:%S %Y')
            C.append(file1)
            D.append(c)
    #print(D)
    #F = (C,D)
    df = pd.DataFrame(C,D)
    df = df.reset_index()
    
    df = df.rename(columns = {'index': 'Date', 0: 'File'}) 
    df['Date'] = (df['Date'].apply(lambda x : datetime.date(x)))
    return df



list_of_files(directory2)


time_monitoring(directory2)



dl.head()

