
import sys
import struct
import socket
import ctypes

mode = 'ITERUNPACK'

struct_fmt = struct.Struct("I 5H")
buf = ctypes.create_string_buffer(struct_fmt.size * 2000) # NOTE: I never ended up doing the experiment with buf and unpack_from... some day, perhaps, although I don't expect to receive huge gains in timing

# all_data = open(sys.argv[1], 'rb').read()
# all_data_fp = open(sys.argv[1], 'rb')
with open(sys.argv[1], 'rb') as all_data_fp:
    # this_val = all_data_fp.read()
    while True:
        if mode == 'SIMPLE': # Took 17s to process one round's worth of data
            data_chunk = all_data_fp.read(struct_fmt.size)

            # In python, once EoF is reached, we'll just read an emptry string (even for binary files), so check if chunk size is 0
            if len(data_chunk) == 0:
                break

            ipid, sent, succ, au, err, loss = struct_fmt.unpack(data_chunk)

            ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

            sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent, succ, au, err, loss) )
            
            data_chunk = ''

        elif mode == 'ITERUNPACK': # Took 13s to process one round's worth of data. 
            data_chunk = all_data_fp.read(struct_fmt.size * 2000) # data_chunk here is now more like a buffer

            # sys.stderr.write("{0}\n".format(len(data_chunk)) )

            # In python, once EoF is reached, we'll just read an emptry string (even for binary files), so check if chunk size is 0. This didn't work for iter_unpack for some reason, so I replaced the test for empty string with the test for len(data_chunk) == 0.            
            if len(data_chunk) == 0:
                break

            gen = struct_fmt.iter_unpack(data_chunk)

            for elem in gen:

                ipid, sent, succ, au, err, loss = elem
            
                ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

                sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent, succ, au, err, loss) )

            data_chunk = ''
    
    # sys.exit(1)
