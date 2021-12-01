#!/usr/bin/env python3

# TODO: This script should probably read the output of stitch_together_ts.py instead of going through each ts_rda file...
# NOTE: This script is for general P(D) analysis that we may want to perform *independent* of the binomial test. I will still be calculating P(D) in binomially_detect_corrfails.py separately from whatever I do here. Need to make sure that the functions I use in both scripts are consistent.

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


def populate_aggr_to_round_status(fname, aggr_to_rd):
    if 'gz' in fname:
        fp = wandio.open(fname)
    else:
        fp = open(fname)

    for line in fp:
        parts = line.strip().split('|')
        aggr = parts[0]

        if aggr not in reqd_aggrs:
            continue

        n_d = int(parts[2])
        n_r = int(parts[3])

        if aggr not in aggr_to_rd:
            # If we want to calculate P(d) for every 24-h bin, we would need to init a dict for every 24h
            # We would also need to know what the 24h-bin this round corresponds to. 
            aggr_to_rd[aggr] = {"r" : 0, "d" : 0}

        aggr_to_rd[aggr]["r"] += n_r
        aggr_to_rd[aggr]["d"] += n_d
        

campaign = sys.argv[1]
inp_path = sys.argv[2]
tstart = int(sys.argv[3])
tend = int(sys.argv[4])

IS_INPUT_COMPRESSED = 0

# Testing
reqd_aggrs = set()
reqd_aggrs.add("projects.zeusping.test1.geo.netacuity.NA.US.4430.asn.22773") # LA Cox

op_dir = "./data/pd/" # TODO: Change this location at some point
op_fname = "{0}/pdperaggr-{1}-{2}to{3}".format(op_dir, campaign, tstart, tend)

aggr_to_rd = {}

for this_t in range(tstart, tend, zeusping_helpers.ROUND_SECS):
    if IS_INPUT_COMPRESSED == 1:
        this_t_fname = "{0}/{1}_to_{2}/ts_rda_test.gz".format(inp_path, this_t, this_t + zeusping_helpers.ROUND_SECS)
    else:
        this_t_fname = "{0}/{1}_to_{2}/ts_rda_test".format(inp_path, this_t, this_t + zeusping_helpers.ROUND_SECS)
    sys.stderr.write("Processing {0} at {1}\n".format(this_t_fname, str(datetime.datetime.now() ) ) )
    populate_aggr_to_round_status(this_t_fname, aggr_to_rd)

    
op_fp = open(op_fname, 'w')
for aggr in aggr_to_rd:

    this_d = aggr_to_rd[aggr]
    op_fp.write("{0} ".format(aggr) )
    # If we are writing this for each 24h bin, then we would be in a for loop for each 24h
    op_fp.write("{0}-{1}\n".format(this_d["d"], this_d["r"]) )
                
op_fp.close()
