
#  This software is Copyright (c) 2019 The Regents of the University of
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

# We will read Feb 17 to Feb 28th data from /scratch/zeusping/data/processed_op_TX_CO_VT_RI_MD_DE_testbintest2/ (1613541600 to 1614470400). We will read the ts_rda_test file for each 10-min round to obtain the dropouts, responsive, anti-dropouts in each round.
# We will use files from mid-March (1615680000 to 1616284800) to calculate the "typical" resps per county (using the median)
# Output file will be ts_rda_test, but with an extra field, indicating median responses.

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
import re

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

def populate_aggr_to_round_status(fname, aggr_to_resps):
    if 'gz' in fname:
        fp = wandio.open(fname)
    else:
        fp = open(fname)

    for line in fp:

        parts = line.strip().split('|')

        aggr_long = parts[0].strip()
        # Skip every aggr that is not TX
        if "NA.US.4431" not in aggr_long:
            continue

        aggr = aggr_long[exp_pref_len:]
        n_r = int(parts[3])
        aggr_to_resps[aggr].append(n_r)


def print_aggr_to_round_status(fname, aggr_to_med, op_fp, this_t):
    if 'gz' in fname:
        fp = wandio.open(fname)
    else:
        fp = open(fname)

    for line in fp:

        parts = line.strip().split('|')

        aggr_long = parts[0].strip()
        # Skip every aggr that is not TX
        if "NA.US.4431" not in aggr_long:
            continue

        aggr = aggr_long[exp_pref_len:]
        n_d = int(parts[2])
        n_r = int(parts[3])
        n_a = int(parts[4])

        if aggr not in aggr_to_med:
            # sys.stderr.write("aggr not in aggr_to_med. aggr {0}\n".format(aggr) )
            # sys.exit(1)
            # I only found a single aggr without a median value (NA.US.4431.3001.asn.6300), so not printing this out anymore
            pass
        else:

            if ".asn." in aggr:
                asn_parts = aggr.strip().split(".asn.")
                aggr_fqdn = asn_parts[0]
                asn = asn_parts[1]
                if aggr_fqdn == "NA.US.4431":
                    aggr_hr = "TX,AS{0}".format(asn)
                else:
                    # if aggr_fqdn == "NA.US.4431.3082":
                    #     sys.stderr.write("{0},{1}\n".format(aggr_fqdn, county_fqdn_to_name[aggr_fqdn]) )
                        
                    aggr_hr = "{0},AS{1}".format(county_fqdn_to_name[aggr_fqdn], asn)
            else:
                if aggr == "NA.US.4431":
                    aggr_hr = "TX"
                else:
                    aggr_hr = county_fqdn_to_name[aggr]

            if this_t not in tstamp_to_tstr:
                this_t_hr = str(datetime.datetime.utcfromtimestamp(this_t) )
                tstamp_to_tstr[this_t] = this_t_hr
            
            op_fp.write("{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}\n".format(this_t, tstamp_to_tstr[this_t], aggr, aggr_hr, n_d, n_r, n_a, int(aggr_to_med[aggr]) ) )

            
tstamp_to_tstr = {}
county_dets_fname = "/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz"
county_dets_fp = wandio.open(county_dets_fname)

county_fqdn_to_name = {}
for line in county_dets_fp:
    parts = line.strip().split(',')
    fqdn = parts[1]
    if fqdn[:10] == "NA.US.4431":
        county_name_temp = parts[2][1:-1] # Remove quotes
        county_name = "TX-{0}".format(county_name_temp)

        # if fqdn == "NA.US.4431.3082":
        #     sys.stderr.write("{0}\n".format(county_name) )
        
        county_fqdn_to_name[fqdn] = county_name
            

op_dir = "./data/"
op_fname = "{0}/tx_per_county".format(op_dir)
inp_path = "/scratch/zeusping/data/processed_op_TX_CO_VT_RI_MD_DE_testbintest2/"

exp_pref_len =len("projects.zeusping.test1.geo.netacuity.")

aggr_to_resps = defaultdict(list)
for this_t in range(1615680000, 1616284800, zeusping_helpers.ROUND_SECS):
    this_t_fname = "{0}/{1}_to_{2}/ts_rda_test".format(inp_path, this_t, this_t + zeusping_helpers.ROUND_SECS)
    # sys.stderr.write("Processing {0} at {1}\n".format(this_t_fname, str(datetime.datetime.now() ) ) )
    populate_aggr_to_round_status(this_t_fname, aggr_to_resps)

    

aggr_to_med = {}
for aggr in aggr_to_resps:
    # this_s24_resp_vals = s24_to_resps[s24]['r']
    # pprint.pprint(s24)
    # pprint.pprint(s24_to_resps[s24])
            
    med = statistics.median(aggr_to_resps[aggr])

    # op_fp.write("{0}|{1}\n".format(s24, math.floor(med) ) )
    # op_fp.write("{0}|{1}\n".format(aggr, med ) )
    aggr_to_med[aggr] = med
    

op_fp = open(op_fname, 'w')
for this_t in range(1613541600, 1614470400, zeusping_helpers.ROUND_SECS):
    this_t_fname = "{0}/{1}_to_{2}/ts_rda_test".format(inp_path, this_t, this_t + zeusping_helpers.ROUND_SECS)
    # sys.stderr.write("Processing {0} at {1}\n".format(this_t_fname, str(datetime.datetime.now() ) ) )
    print_aggr_to_round_status(this_t_fname, aggr_to_med, op_fp, this_t)


op_fp.close()

