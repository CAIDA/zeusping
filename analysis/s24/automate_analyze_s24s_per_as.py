
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
inp_path = sys.argv[2]
tstart = int(sys.argv[3])
tend = int(sys.argv[4])
usstate_to_reqd_asns_fname = sys.argv[5]

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

for usstate in usstate_to_reqd_asns:
    # if usstate != 'FL':
    #     continue
    
    for asn in usstate_to_reqd_asns[usstate]:

        # if asn != '7922':
        #     continue
        
        # First get the addresses we care about
        # awk -F '|' '{if ( ($5 == 7922) && ($6 == "Florida") ) print}' LA_MS_AL_AR_FL_CT_pseudo_addrtodropouts_detailed > LA_MS_AL_AR_FL_CT-FL-AS7922-pingedaddrs
        awk_cmd = """awk -F '|' '{if ( ($5 == """
        awk_cmd += """{0}) && """.format(asn)
        awk_cmd += """($6 == """
        awk_cmd += """ "{0}") )""".format(usstate_code_to_name[usstate])
        awk_cmd += """ print}'"""
        awk_cmd += """ ./data/{2}_pseudo_addrtodropouts_detailed > ./data/{2}-{3}-AS{0}-pingedaddrs""".format(asn, usstate_code_to_name[usstate], campaign, usstate)
        # awk_cmd = """awk -F '|' '{if ( (\$5 == {0}) && (\$6 == "{1}") ) print}' {2}_pseudo_addrtodropouts_detailed > {2}-{3}-AS{0}-pingedaddrs""".format(asn, usstate_code_to_name[usstate], campaign, usstate)
        sys.stderr.write("{0}\n".format(awk_cmd) )
        os.system(awk_cmd)

        sys.stderr.write("Beginning run for {0} {1} at {2}\n".format(usstate, asn, str(datetime.datetime.now() ) ) )
        cmd = """python analyze_s24s_per_as.py mr-multiround {0} {1} {2} {3} {4} {5}""".format(campaign, usstate, asn, inp_path, tstart, tend)
        sys.stderr.write("{0}\n".format(cmd) )
        os.system(cmd)
