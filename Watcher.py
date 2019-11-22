import sys
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from datetime import datetime

class MyHandler(PatternMatchingEventHandler):
    
    #All files
    patterns=  "*"

    #Informs change for files and directories      
    def _process(self, event):
        print(event.src_path, event.event_type, datetime.now())
        #with open('out.txt', 'w+') as f:
            #print('Filename:', event.src_path, event.event_type, file=f)


    #Checks when files or directory was moved
    def _on_moved(self, event):
        self._process(event)

    #Checks when files or directory was deleted
    def _on_deleted(self, event):
        self._process(event)

    #Checks when files or directory was modified
    def _on_modified(self, event):
        self._process(event)
    
    #Checks when files or directory was created
    def _on_created(self, event):
        self._process(event)


#All subdirectories are considered
# Runs on the background until stopped   

if __name__ == '__main__':
    start = '/bigdata'
    observer = Observer()
    observer.schedule(MyHandler(), path=start, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


#sudo mount 10.112.1.10:/storage/bigdata /bigdata