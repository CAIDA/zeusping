
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
rda_path = sys.argv[2]
pd_calc_tstart = int(sys.argv[3])
pd_calc_tend = int(sys.argv[4])
usstate_to_reqd_asns_fname = sys.argv[5]
tstart = int(sys.argv[6])
tend = int(sys.argv[7])

statecode_to_fqdn = {}
# I need to map each region to its netacuity fqdn. 
regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
regions_fp = wandio.open(regions_fname)
for line in regions_fp:
    parts = line.strip().split(',')

    fqdn = parts[1].strip()
    # TODO: We will have to do this differently for other countries
    loccode = parts[3]

    if loccode[:3] == "US.":
        statecode = loccode[3:]
        statecode_to_fqdn[statecode] = fqdn


usstate_to_reqd_asns = {}
populate_usstate_to_reqd_asns(usstate_to_reqd_asns_fname, usstate_to_reqd_asns)

# pprint.pprint(usstate_to_reqd_asns)
# sys.exit(1)

usstate_code_to_name = {
    "LA" : 'Louisiana',
    "MS" : 'Mississippi',
    "AL" : 'Alabama',
    "AR" : 'Arkansas',
    "FL" : 'Florida',
    "CT" : 'Connecticut',
    "TX" : 'Texas',
    "CO" : 'Colorado',
    "VT" : 'Vermont',
    "RI" : 'Rhode Island',
    "MD" : 'Maryland',
    "DE" : 'Delaware',
    "CA" : 'California',
    "ME" : 'Maine',
    }


done_fp = open('./data/pre_crispr_usenix21/binomially_done_state_ases', 'a')
for usstate in usstate_to_reqd_asns:
    # if usstate != 'CA':
    #     continue
    
    for asn in usstate_to_reqd_asns[usstate]:

        # if usstate == 'CA':
        #     if ( (asn != '20001') and (asn != '7922') and (asn != '5650') ):
        #         continue

        # if usstate == 'ME':
        #     if ( (asn != '11351') and (asn != '7922') ):
        #         continue
            
        file_for_pd_calc = "{0}/sr_rda_per_round_{1}to{2}/rda_per_round_geo.netacuity.{3}.asn.{4}".format(rda_path, pd_calc_tstart, pd_calc_tend, statecode_to_fqdn[usstate], asn)
        rda_sr_per_aggr_fname = "{0}/sr_rda_per_round_{1}to{2}/rda_per_round_geo.netacuity.{3}.asn.{4}".format(rda_path, tstart, tend, statecode_to_fqdn[usstate], asn)
        rda_mr_per_aggr_fname = "{0}/mr_rda_per_round_{1}to{2}/rda_per_round_geo.netacuity.{3}.asn.{4}".format(rda_path, tstart, tend, statecode_to_fqdn[usstate], asn)

        sys.stderr.write("Beginning run for {0} {1} at {2}\n".format(usstate, asn, str(datetime.datetime.now() ) ) )
        cmd = """python binomially_detect_corrfails.py {0} {1} {2} {3} {4} {5} {6} {7} {8}""".format(campaign, usstate, asn, rda_path, file_for_pd_calc, pd_calc_tstart, pd_calc_tend, rda_sr_per_aggr_fname, rda_mr_per_aggr_fname)
        sys.stderr.write("{0}\n".format(cmd) )
        args = shlex.split(cmd)
        # os.system(cmd)

        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            sys.stderr.write("cmd failed for f {0}; exiting\n".format(fil) )
            # continue
            sys.exit(1)

        done_fp.write("Binomially processed {0} {1} for {2} to {3} at {4}\n".format(usstate, asn, tstart, tend, str(datetime.datetime.now() ) ) )
        done_fp.flush()

