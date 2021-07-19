
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


def find_reqd_ips(reqd_ips_fname):

    reqd_ips = set()
    
    fp = open(reqd_ips_fname)
    for line in fp:

        parts = line.strip().split('|')
        addr = parts[0].strip()

        reqd_ips.add(addr)

    fp.close()
    return reqd_ips

    
ip_fname = sys.argv[1]
ip_fp = wandio.open(ip_fname)

reqd_ips_fname = sys.argv[2]
reqd_ips = find_reqd_ips(reqd_ips_fname)

for line in ip_fp:
    parts = line.strip().split()

    addr = parts[0].strip()

    if addr not in reqd_ips:
        continue

    status = int(parts[1])

    if status == 0:
        sys.stdout.write("{0}\n".format(addr) )
    

