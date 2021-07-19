
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
else:
    py_ver = 3

ip_fname = sys.argv[1]
ip_fp = open(ip_fname)

reqd_s24 = sys.argv[2]
# op_fp = open(sys.argv[2], 'w')

# reqd_s24s_fname = sys.argv[3]

# I replaced this function with find_addrs_in_s24_with_status from zeusping_helpers.py
# def find_set_bits(s24, val):


#     s24_pref = s24[:-4]
    
#     s24_to_set_bits = defaultdict(int)
#     curr_oct4 = 0
#     for bit_pos in range(256):

#         if( ( (val >> bit_pos) & 1) == 1):
#             oct4 = "{0}{1}".format(s24_pref, curr_oct4)
#             s24_to_set_bits[oct4] = 1

#         curr_oct4 += 1

#     return s24_to_set_bits


        


for line in ip_fp:
    parts = line.strip().split('|')

    s24 = parts[0].strip()

    if s24 != reqd_s24:
    # if s24 != '142.202.88.0/24':
        continue

    s24_to_dets = defaultdict(set)
    
    d_addrs = int(parts[1])
    zeusping_helpers.find_addrs_in_s24_with_status(s24, d_addrs, 'd', s24_to_dets)
    
    r_addrs = int(parts[2])
    zeusping_helpers.find_addrs_in_s24_with_status(s24, r_addrs, 'r', s24_to_dets)
    
    a_addrs = int(parts[3])
    zeusping_helpers.find_addrs_in_s24_with_status(s24, a_addrs, 'a', s24_to_dets)    
    # sys.exit(1)

statuses = ['d', 'r', 'a']
for status in statuses:
    if status in s24_to_dets:
        for addr in s24_to_dets[status]:
            sys.stdout.write("{0} {1}\n".format(addr, status) )
            # op_fp.write("{0} {1} {2}\n".format(oct4, s24_to_set_bits[oct4], status) )
    
