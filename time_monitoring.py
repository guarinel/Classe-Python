import os 
import os.path, time

directory2 = '/bigdata'

def subdirectories(pasta):
    A = (list([x[0] for x in os.walk(pasta)]))
    return A


def time_monitoring(P_directory):
    for directory in subdirectories(P_directory):
        for file1 in os.listdir(directory):
            print(directory + ": " + file1)
            print("last modified: %s" % time.ctime(os.path.getmtime(directory +'/'+ file1)))
            print("created: %s" % time.ctime(os.path.getctime(directory +'/'+ file1)))

time_monitoring(directory2)

