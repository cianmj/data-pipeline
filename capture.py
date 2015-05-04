import sys
import boto
import argparse


from boto.kinesis.exceptions import ProvisionedThroughputExceededException


from KinesisWorker import KinesisWorker

kinesis = boto.connect_kinesis()
iter_type_at = 'AT_SEQUENCE_NUMBER'
iter_type_after = 'AFTER_SEQUENCE_NUMBER'
iter_type_trim = 'TRIM_HORIZON'
iter_type_latest = 'LATEST'


def main():
    stream_name = 'eeg'
    worker_time = 90
    sleep_interval = 0.5
    
    stream = kinesis.describe_stream(stream_name)
    #print (json.dumps(stream, sort_keys=True, indent=2, separators=(',', ': ')))
    shards = stream['StreamDescription']['Shards']
    #print ('# Shard Count:', len(shards))

    threads = []
    #start_time = datetime.datetime.now()
    for shard_id in xrange(len(shards)):
        worker_name = 'shard_worker:%s' % shard_id
        print ('#-> shardId:', shards[shard_id]['ShardId'])
        worker = KinesisWorker(
            stream_name=stream_name,
            shard_id=shards[shard_id]['ShardId'],
            # iterator_type=iter_type_trim,  # uses TRIM_HORIZON
            iterator_type=iter_type_latest,  # uses LATEST
            worker_time=worker_time,
            sleep_interval=sleep_interval,
            echo=True,
            name=worker_name
            )
        worker.daemon = True
        threads.append(worker)
        print ('#-> starting: ', worker_name)
        worker.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    main()