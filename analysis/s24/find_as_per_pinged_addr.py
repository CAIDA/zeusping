
#  This software is Copyright (c) 2021 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes,
#  without fee, and without a written agreement is hereby granted, provided that
#  the above copyright notice, this paragraph and the following three paragraphs
#  appear in all copies. Permission to make use of this software for other than
#  academic research and education purposes may be obtained by contacting:
#
#  Office of Innovation and Commercialization
#  9500 Gilman Drive, Mail Code 0910
#  University of California
#  La Jolla, CA 92093-0910
#  (858) 534-5815
#  invent@ucsd.edu
#
#  This software program and documentation are copyrighted by The Regents of the
#  University of California. The software program and documentation are supplied
#  "as is", without any accompanying services from The Regents. The Regents does
#  not warrant that the operation of the program will be uninterrupted or
#  error-free. The end-user understands that the program was developed for research
#  purposes and is advised not to rely exclusively on the program for any reason.
#
#  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
#  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
#  PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
#  THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
#  IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
#  MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

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
