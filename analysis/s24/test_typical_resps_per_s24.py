
import sys
import struct
import socket

test_fp = open(sys.argv[1])

for line in test_fp:
    parts = line.strip().split('|')

    ipint = int(parts[0])
    resp = parts[1]

    ipstr = socket.inet_ntoa(struct.pack('!L', ipint))

    sys.stdout.write("{0}/24|{1}\n".format(ipstr, resp) )
