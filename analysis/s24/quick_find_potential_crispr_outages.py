
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
import wandio
import math
import pprint

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


def populate_usstate_to_reqd_asns(usstate_to_reqd_asns_fname, usstate_to_reqd_asns):
    usstate_to_reqd_asns_fp = open(usstate_to_reqd_asns_fname)

    for line in usstate_to_reqd_asns_fp:
        parts = line.strip().split()
        usstate = parts[0].strip()

        if usstate not in usstate_to_reqd_asns:
            usstate_to_reqd_asns[usstate] = set()

        asn_list = parts[1].strip()
        asns = asn_list.strip().split('-')

        for asn in asns:
            asns_reqd_splits_parts = asn.strip().split(':')
            usstate_to_reqd_asns[usstate].add(asns_reqd_splits_parts[0])


campaign = sys.argv[1]
tstart = int(sys.argv[2])
tend = int(sys.argv[3])
inp_path = sys.argv[4]
usstate_to_reqd_asns_fname = sys.argv[5]

# Define nested_dicts for usstate_asn_s24_outages files
def nested_dict_factory_int(): 
  return defaultdict(int)

def nested_dict_factory(): 
  return defaultdict(nested_dict_factory_int)

usstate_to_reqd_asns = {}
populate_usstate_to_reqd_asns(usstate_to_reqd_asns_fname, usstate_to_reqd_asns)

usstate_asn_s24_outages = defaultdict(nested_dict_factory)
out_t_to_str = {}

for usstate in usstate_to_reqd_asns:
    # if usstate != 'FL':
    #     continue
    
    for asn in usstate_to_reqd_asns[usstate]:

        inp_fname = '{0}/knockedouts24s-{1}-{2}-AS{3}-{4}to{5}'.format(inp_path, campaign, usstate, asn, tstart, tend)
        try:
            inp_fp = open(inp_fname)
        except IOError:
            sys.stderr.write("No file for usstate, asn: {0}, {1}\n".format(usstate, asn) )

        for line in inp_fp:
            parts = line.strip().split()

            out_t = int(parts[0].strip() )

            out_str = "{0} {1}".format(parts[1], parts[2])

            out_t_to_str[out_t] = out_str

            usstate_asn_s24_outages[usstate][asn][out_t] += 1

            
op_fname = '{0}/potcrispr-{1}-{2}to{3}'.format(inp_path, campaign, tstart, tend)
op_fp = open(op_fname, 'w')
            
for usstate in usstate_asn_s24_outages:
    for asn in usstate_asn_s24_outages[usstate]:
        for out_t in usstate_asn_s24_outages[usstate][asn]:

            if usstate_asn_s24_outages[usstate][asn][out_t] >= 10:
                op_fp.write("{0} {1} {2} {3} {4}\n".format(usstate_asn_s24_outages[usstate][asn][out_t], usstate, asn, out_t, out_t_to_str[out_t]) )
