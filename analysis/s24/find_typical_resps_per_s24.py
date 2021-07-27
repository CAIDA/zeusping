

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
import statistics
import math
import pprint

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

def populate_s24_to_round_status_sr(fname, s24_to_resps):
    if 'gz' in fname:
        fp = wandio.open(fname)
    else:
        fp = open(fname)

    for line in fp:

        parts = line.strip().split('|')

        s24 = parts[0].strip()

        r_addrs = int(parts[2])
        s24_to_dets = defaultdict(set)
        zeusping_helpers.find_addrs_in_s24_with_status(s24, r_addrs, 'r', s24_to_dets)

        s24_to_resps[s24].append(len(s24_to_dets['r']) )


inp_path = sys.argv[1]
tstart = int(sys.argv[2])
tend = int(sys.argv[3])
op_fname = sys.argv[4]

s24_to_resps = defaultdict(list)
for this_t in range(tstart, tend, zeusping_helpers.ROUND_SECS):
    this_t_fname = "{0}/{1}_to_{2}/ts_s24_sr_test".format(inp_path, this_t, this_t + zeusping_helpers.ROUND_SECS)
    sys.stderr.write("Processing {0} at {1}\n".format(this_t_fname, str(datetime.datetime.now() ) ) )
    populate_s24_to_round_status_sr(this_t_fname, s24_to_resps)

    
op_fp = open(op_fname, 'w')    
for s24 in s24_to_resps:
    # this_s24_resp_vals = s24_to_resps[s24]['r']
    # pprint.pprint(s24)
    # pprint.pprint(s24_to_resps[s24])
            
    med = statistics.median(s24_to_resps[s24])

    # op_fp.write("{0}|{1}\n".format(s24, math.floor(med) ) )
    op_fp.write("{0}|{1}\n".format(s24, med ) )

op_fp.close()
