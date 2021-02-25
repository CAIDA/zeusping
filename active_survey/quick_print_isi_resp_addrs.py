#!/usr/bin/env python

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime


def print_isi_addrs(num_hists):

    line_ct = 0

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
        # hist = hist[-1] # NOTE: Hack to make this faster temporarily

        hex_int = int(hist, 16)

        resp = 0

        for i in range(num_hists):
            if hex_int & 1 == 1:
                resp = 1

            hex_int = hex_int >> 1

        # print ip, hist, hex_int, resp

        if resp == 1:
            resp_fp.write("{0}\n".format(ip) )
        elif resp == 0:
            unresp_fp.write("{0}\n".format(ip) )


resp_fp = open(sys.argv[1], 'w')
unresp_fp = open(sys.argv[2], 'w')
# num_hists is the number of historical censuses we will consider to determine if an address was respsonsive
num_hists = int(sys.argv[3])
print_isi_addrs(num_hists)
    
    
