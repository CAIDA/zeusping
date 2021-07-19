
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

    
def find_unusual_s24s(reqd_s24_fname):

    reqd_s24s = set()
    
    reqd_s24_fp = open(reqd_s24_fname)

    for line in reqd_s24_fp:
        parts = line.strip().split()

        s24 = parts[0].strip()
        n_pinged = int(parts[1].strip() )
        n_d = int(parts[2].strip() )
        n_r = int(parts[3].strip() )

        if n_d >= 10 and n_r == 1:
            reqd_s24s.add(s24)

    return reqd_s24s

            
ip_fname = sys.argv[1]
ip_fp = open(ip_fname)

reqd_s24_fname = sys.argv[2]
reqd_s24s = find_unusual_s24s(reqd_s24_fname)


for line in ip_fp:
    parts = line.strip().split('|')

    s24 = parts[0].strip()

    if s24 not in reqd_s24s:
        continue

    r_addrs = int(parts[2])

    s24_to_dets = defaultdict(set)
    zeusping_helpers.find_addrs_in_s24_with_status(s24, r_addrs, 'r', s24_to_dets)

    for addr in s24_to_dets['r']:
        sys.stdout.write("{0}\n".format(addr) )
    

