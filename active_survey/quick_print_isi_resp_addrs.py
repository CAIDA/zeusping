#!/usr/bin/env python

# NOTE: Parts of this script are based off find_timeseries_pts.py

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime


def print_isi_addrs(op_fp):

    line_ct = 0

    NUM_HISTS = 2 # How many historical censuses we will consider to determine if an address was respsonsive
    
    for line in sys.stdin:

        line_ct += 1

        if line_ct == 1:
            continue

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} isi_hitlist lines read at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )
        
        # if line_ct > 1000:
        #     sys.exit(1)
        
        parts = line.strip().split()

        ip = parts[1].strip()

        hist = parts[2].strip()
        hist = hist[-1] # NOTE: Hack to make this faster temporarily

        hex_int = int(hist, 16)

        resp = 0

        for i in range(NUM_HISTS):
            if hex_int & 1 == 1:
                resp = 1

            hex_int = hex_int >> 1

        # print ip, hist, hex_int, resp

        if resp == 1:
            op_fp.write("{0}\n".format(ip) )


op_fp = open(sys.argv[1], 'w')
print_isi_addrs(op_fp)
    
    
