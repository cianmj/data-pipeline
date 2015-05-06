import subprocess
import threading
import time

from os import listdir
from os.path import isfile, join

import numpy as np

def bash_command(cmd):
    subprocess.Popen(cmd, shell=True, executable='/bin/bash')


class StartProcess(threading.Thread):
    def __init__(self,cmd):
        self.stdout = None
        self.stderr = None
        self.cmd = cmd
        threading.Thread.__init__(self)

    def run(self):
        p = subprocess.Popen(self.cmd.split(),
                                shell=False)
                                #stdout=subprocess.PIPE,
                                #stderr=subprocess.PIPE)
                                #stdin=subprocess.PIPE)
        stdout, stderr = p.communicate()


def start_client_server(port,filename):
    start_server = 'python server.py -p '+str(port)
    bash_command(start_server)

    start_client = 'python client.py -p '+str(port)+' -f '+str(filename)
    bash_command(start_client)


mypath = 'rawdata'
files = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]

np.random.shuffle(files)

port = 10000
threads = []
for i,file in enumerate(files[0:10]):
#for i,file in enumerate(['tmp.csv']):
    #filename = str(file)
    filename = 'rawdata/'+str(file)
    
    server = StartProcess(cmd='python server.py -p '+str(port))
    client = StartProcess(cmd='python client.py -p '+str(port)+' -f '+str(filename))

    server.start()
    time.sleep(1)
    client.start()

    threads.append(server)
    threads.append(client)


    port +=1 

