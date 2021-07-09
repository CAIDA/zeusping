
import sys
import glob
import shlex
import subprocess
import os
import datetime
import json
from collections import defaultdict
import array
import io
import struct
import socket
import radix
import gmpy
import pyipmeta

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

inp_file1 = sys.argv[1]
inp_file2 = sys.argv[2]
pfx2AS_fn = sys.argv[3]

rtree = radix.Radix()
rnode = zeusping_helpers.load_radix_tree(pfx2AS_fn, rtree)

def process_file(fname):
    inp_fp = open(fname)

    for line in inp_fp:
        addr = line[:-1]

        asn = 'UNK'
        # Find ip_to_as, ip_to_loc
        rnode = rtree.search_best(addr)
        if rnode is None:
            asn = 'UNK'
        else:
            asn = rnode.data["origin"]

        ip_to_as[addr] = asn

        
ip_to_as = {}
        
process_file(inp_file1)
process_file(inp_file2)

for ip in ip_to_as:
    sys.stdout.write("{0}|{1}\n".format(ip, ip_to_as[ip]) )
