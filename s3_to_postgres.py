import socket
import urllib2
import time

import gzip
import pandas as pd
from StringIO import StringIO

#import psycopg2

from sqlalchemy import create_engine

from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key
conn = S3Connection()

from collections import OrderedDict

def s3_connection():
    try:
        bucket = conn.get_bucket('eeg-testing')#,location=Location.DEFAULT)
    except Exception, e:
        print 'Could not connect to s3'
        print e 
        raise
    return bucket


def get_s3_keys(bucket):
    #key = s3_connection()
    key_dict = OrderedDict()
    keys = [key.name for key in list(bucket.list())]

    for i,key in enumerate(keys):
        tmp = key.split('/')
        cid = tmp[1]
        if cid in key_dict:
            key_dict[cid].append(key)
        else:
            key_dict[cid] = [key]
    return key_dict


def postgres_connection():
    try:
        engine = create_engine('postgresql://ubuntu:postgres@localhost/eeg', echo=True)
    except Exception, e:
        print 'Could not connect to postgres database'
        print e
        raise
    return engine

# Raw Data Structure
# subject_id, status_id, sampling_rate, matching_condition, trial_number, electrode_location, sample_number, sensor_value
def load_data(bucket,key_dict):
    engine = postgres_connection()

    columns = ['subject_id','status_id','sampling_rate','matching_condition','trial_number','electrode_location','sample_number','sensor_value']
    for cid,s3_keys in key_dict.iteritems():
        for keys in s3_keys:
            print cid,keys
            key = bucket.get_key(keys)#,validate=False)
            gzip_file = gzip.GzipFile(fileobj=(StringIO(key.get_contents_as_string())))
            data = gzip_file.read().split(';')
            data = [val.split(',') for val in data]
            df = pd.DataFrame(data,columns=columns)
            df.to_sql('eeg_raw', con=engine, index=False, if_exists='append', chunksize=20000) #schema='prod')


if __name__ == '__main__':

    bucket = s3_connection()

    key_dict = get_s3_keys(bucket)

    load_data(bucket,key_dict)


#
#sqlString = "SELECT * FROM SOMETABLE"    # SOMETABLE is a view in mySchemaName 
#df = pd.read_sql(sqlString, con=engine) 
#