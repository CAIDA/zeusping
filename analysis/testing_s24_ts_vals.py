
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


for line in ip_fp:
    parts = line.strip().split('|')

    s24 = parts[0].strip()

    if s24 != '198.72.211.0/24':
        continue

    n_d = int(parts[1])

    if n_d > 0:
        s24_to_set_bits = find_set_bits(s24, n_d)
        
        for oct4 in s24_to_set_bits:
            sys.stdout.write("{0} {1}\n".format(oct4, s24_to_set_bits[oct4]) )

        sys.exit(1)
