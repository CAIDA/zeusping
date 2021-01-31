
import sys
import struct
import socket

struct_fmt = struct.Struct("I 5H")

# all_data = open(sys.argv[1], 'rb').read()
# all_data_fp = open(sys.argv[1], 'rb')
with open(sys.argv[1], 'rb') as all_data_fp:
    # this_val = all_data_fp.read()
    while True:
        data_chunk = all_data_fp.read(struct_fmt.size)

        # In python, once EoF is reached, we'll just read an emptry string (even for binary files)
        if data_chunk == '':
            break
        
        ipid, sent, succ, au, err, loss = struct_fmt.unpack(data_chunk)
        ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

        sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent, succ, au, err, loss) )
    
    # sys.exit(1)
