import socket
import urllib2
import time

import gzip

import psycopg2
from sqlalchemy import create_engine

from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key
conn = S3Connection()

def s3_connection():
	try:
	    bucket = conn.get_bucket('eeg-testing',location=Location.DEFAULT)
	    k = Key(bucket)
	except, Exception e:
		print 'Could not connect to s3'
	    print e 
	    raise
	return k

def get_s3_data():

	key = s3_connection()




def postgres_connection():
	try:
		engine = create_engine('postgresql://ubuntu:postgres@localhost/eeg', echo=True)
	except, Exception e:
		print 'Could not connect to postgres database'
		print e
		raise




#df.to_sql('table_name', con=engine, index=False, if_exists='append', chunksize=20000)




#
#sqlString = "SELECT * FROM SOMETABLE"    # SOMETABLE is a view in mySchemaName 
#df = pd.read_sql(sqlString, con=engine) 