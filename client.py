import socket
import sys
import time

def main(port,filename):
    # Create a UDP socket
    print >>sys.stderr, 'Opening socket'
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    server_address = ('localhost', port)
    message = 'Reading and transmitting data'
    #
    # Data Structure
    # subject_id, status_id, sampling_rate, matching_condition, trial_number, electrode_location, sample_number, sensor_value
    #
    try:
        # Send data
        print message + ' from ' + filename
        loop = -1
        with open(filename) as f:
            for line in f:
                loop +=1
                line = line.split(' ')
                if loop == 0:
                    subject_id = str(line[1].split('.')[0])
                    status_id = subject_id[3]
                    continue
                elif loop == 2:
                    sampling_rate = str(line[1])
                    continue
                elif loop == 3:
                    matching_condition = str(line[1]) + '_' + str(line[2])
                    continue
                elif loop >= 5:
                    data = [subject_id] + [status_id] + [sampling_rate] + [matching_condition] + line
                    data = (','.join(data)).rstrip('\n')
                else:
                    continue

                #print data
                sent = sock.sendto(data, server_address)
                time.sleep(0.1) 

                # Receive response
                print >>sys.stderr, 'Waiting for response...'
                data, server = sock.recvfrom(4096)
                print >>sys.stderr, 'Response received.'

    finally:
        print >>sys.stderr, 'Closing socket'
        sock.close()


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description='Client simulates wearable and sent signals through a socket')
    parser.add_argument('-p','--port',default=10000)
    parser.add_argument('-f','--filename',default='tmp.csv')
    args = parser.parse_args()
    
    port = int(args.port)
    filename = str(args.filename)

    main(port,filename)


