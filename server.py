import socket
import sys
import time
import threading

from collections import OrderedDict

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Bind the socket to the port
server_address = ('localhost', 10000)
print >>sys.stderr, 'starting up on %s port %s' % server_address
sock.bind(server_address)


import boto
from boto.kinesis.exceptions import ResourceNotFoundException
kinesis = boto.connect_kinesis()

from KinesisPoster import KinesisPoster

import gzip
from cStringIO import StringIO

# buf = StringIO()
# f = gzip.GzipFile(mode='wb', fileobj=buf)
# try:
#     f.write(uncompressed_data)
# finally:
#     f.close()
# content = "Lots of content here"
# f = gzip.open('file.txt.gz', 'wb')
# f.write(content)
# f.close()


def sum_posts(kinesis_actors):
    """Sum all posts across an array of KinesisPosters
    """
    total_records = 0
    for actor in kinesis_actors:
        total_records += actor.total_records
    return total_records
    

def get_or_create_stream(stream_name, shard_count):
    stream = None
    try:
        stream = kinesis.describe_stream(stream_name)
        #print (json.dumps(stream, sort_keys=True, indent=2,separators=(',', ': ')))
    except ResourceNotFoundException as rnfe:
        while (stream is None) or ('ACTIVE' not in stream['StreamDescription']['StreamStatus']):
            if stream is None:
                print ('Could not find ACTIVE stream:{0} trying to create.'.format(
                    stream_name))
                kinesis.create_stream(stream_name, shard_count)
            else:
                print ("Stream status: %s" % stream['StreamDescription']['StreamStatus'])
            time.sleep(1)
            stream = kinesis.describe_stream(stream_name)

    return stream

def data_formating(data):
    data = data.split(',')
    json = {'subject_id':data[0],
            'status_id':data[1],
            'sampling_rate':data[2],
            'matching_condition':data[3],
            'trial_number':data[4],
            'electrode_location':data[5],
            'sample_number':data[6],
            'sensor_value':data[7]
            }
    return json

def kinesis_poster(stream_name,part_key,poster_name,poster_time,data):
    poster = KinesisPoster( stream_name=stream_name,
                            partition_key=part_key,
                            name=poster_name, 
                            poster_time=poster_time,
                            default_records=data,
                            quiet=True)
    poster.daemon = True        
    threads.append(poster)
    print 'Starting ', poster_name
    poster.start()


def gzip_data(data_set,batch_id):
    time.sleep(5) 
    folder = data_set[0].split(',')[0]
    filename = str(folder)+'-'+str(batch_id)+'.gz'
    f = gzip.GzipFile('newdata/'+filename, 'wb')
    f.write(';'.join(data_set))
    f.flush()
    f.close()
    print filename
    return data_set

def main():
    
    #stream_name = 'eeg'
    #shard_count = 1
    #part_key = 'co3c0000402'
    #poster_name = '119'
    #poster_time = 1
    max_bytes = 4096 * 2

    #stream = get_or_create_stream(stream_name, shard_count)
    #print kinesis.describe_stream(stream_name)

    threads = []
    data_set = []
    while True:
        print >>sys.stderr, '\nwaiting to receive message'
        # Capture data with following structure :
        # [subject_id, status_id, sampling_rate, matching_condition, trial_number, electrode_location, sample_number, sensor_value]
        data, address = sock.recvfrom(4096)
        print >>sys.stderr, 'received %s bytes from %s' % (len(data), address)
        print >>sys.stderr, data
        #json = data_formating(data)

        #kinesis_poster(stream_name,part_key,poster_name,poster_time,data)
        data_set.append(data)
        print 'size: ',sys.getsizeof(data_set)
        if sys.getsizeof(data_set) >= max_bytes:
            batch_id = int(time.time())
            t = threading.Thread(target=gzip_data, args=(data_set,batch_id,))
            threads.append(t)
            t.start()
            data_set = []

        if data:
            sent = sock.sendto(data, address)
        #    print >>sys.stderr, 'sent %s bytes back to %s' % (sent, address)




if __name__ == '__main__':
    main()
