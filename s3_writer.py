import socket
import urllib2
import time

from sys import stdout

from os import listdir
from os.path import isfile, join
import shutil

from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key
conn = S3Connection()

#
# Functions to test internet connectivity 
#
def internet_on():
    return is_connected1() | is_connected2()

def is_connected1():
    REMOTE_SERVER = "www.google.com"
    try:
        host = socket.gethostbyname(REMOTE_SERVER)
        s = socket.create_connection((host, 80), 2)
        return True
    except Exception, e: pass
    return False

def is_connected2():
    REMOTE_SERVER2 = "https://aws.amazon.com/"
    try:
        response=urllib2.urlopen(REMOTE_SERVER2,timeout=1)
        return True
    except urllib2.URLError as err: pass
    return False


def load_filename(dir='newdata'):
    files = [ f for f in listdir(dir) if isfile(join(dir,f)) ]
    return files


def pause_function(reason,seconds):
    stdout.write(reason)
    stdout.write("\n") 
    stdout.write('Retrying in '+str(seconds) + ':')
    stdout.write("\n") 
    for i in range(1,seconds):
        stdout.write('\r%d' % i)
        stdout.flush()
        time.sleep(1)
    stdout.write("\n") 


newdata = 'newdata'
olddata = 'olddata'
while True:

    while internet_on():
        try:
            bucket = conn.get_bucket('eeg-testing',location=Location.DEFAULT)
        except:
            bucket = conn.create_bucket('eeg-testing',location=Location.DEFAULT)
        k = Key(bucket)    
        
        files = load_filename(newdata)
        for file in files:
            print file
            filepath = newdata+'/'+file
            try:
                k.key = file.replace('-','/')
                k.set_contents_from_filename(filepath)
                shutil.move(filepath, olddata+'/'+file)
            except:
                continue

        pause_function(reason='Connected, but found no files to transfer.',seconds=30)

    pause_function(reason='No internet connection found.',seconds=60)
    #time.sleep(60)


