
import sys
import os
import datetime
import subprocess
import os
import wandio
import struct
import socket
import ctypes
import shlex
import gmpy
import gc
from collections import defaultdict
import radix
import pyipmeta

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
    from sc_warts import WartsReader
else:
    py_ver = 3

ip_fname = sys.argv[1]
ip_fp = open(ip_fname)

op_fp = open(sys.argv[2], 'w')

# reqd_s24s_fname = sys.argv[3]

def find_set_bits(s24, val):


    s24_pref = s24[:-4]
    
    s24_to_set_bits = defaultdict(int)
    curr_oct4 = 0
    for bit_pos in range(256):

        if( ( (val >> bit_pos) & 1) == 1):
            oct4 = "{0}{1}".format(s24_pref, curr_oct4)
            s24_to_set_bits[oct4] = 1

        curr_oct4 += 1

    return s24_to_set_bits


def write_addr_status(bit_string, status):
    if bit_string > 0:
        s24_to_set_bits = find_set_bits(s24, bit_string)
        
        for oct4 in s24_to_set_bits:
            op_fp.write("{0} {1} {2}\n".format(oct4, s24_to_set_bits[oct4], status) )


for line in ip_fp:
    parts = line.strip().split('|')

    s24 = parts[0].strip()

    if s24 != '24.30.248.0/24':
        continue

    d_addrs = int(parts[1])
    write_addr_status(d_addrs, 'd')

    r_addrs = int(parts[2])
    write_addr_status(r_addrs, 'r')

    a_addrs = int(parts[3])
    write_addr_status(a_addrs, 'a')
    # sys.exit(1)
