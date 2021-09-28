
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
import os
import subprocess
import shlex
import wandio
import pprint

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

if sys.version_info[0] == 2:
    py_ver = 2
else:
    py_ver = 3


def convert_aggr_to_oldfname(aggr):

    if 'asn' in aggr:
        parts = aggr.strip().split('.asn.')
        # sys.stderr.write("{0}\n".format(aggr) )
        # sys.stderr.write("{0} {1}\n".format(parts[0], parts[1]) )
        # sys.exit(1)

        if len(parts) == 1:
            asn = parts[0]
            # Turns out I did not calculate per-AS statistics in the prior scheme
            # oldfname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/responsive_and_dropout_addrs_1617495000to1617753600/resp_dropout_per_round_{AS{1}".format(asn)
            return -1
        else:
            aggr_fqdn = parts[0]
            asn = parts[1]
            oldfname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/responsive_and_dropout_addrs_1617495000to1617753600/resp_dropout_per_round_{0}_AS{1}".format(fqdn_to_oldfname_suf[aggr_fqdn], asn)
        
    else:
        
        oldfname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/responsive_and_dropout_addrs_1617495000to1617753600/resp_dropout_per_round_{0}".format(fqdn_to_oldfname_suf[aggr])
        
    return oldfname


def build_new_ts(new_ts):
    for round_tstamp in range(start_round_epoch, end_round_epoch, zeusping_helpers.ROUND_SECS):
        ts_fname = "/scratch/zeusping/data/processed_op_CA_ME_testbintest3/{0}_to_{1}/ts_rda_test.gz".format(round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)

        fp = wandio.open(ts_fname)

        for line in fp:
            parts = line.strip().split('|')

            aggr_full = parts[0].strip()
            if "routing" not in aggr_full:
                aggr = aggr_full[AGGR_PREF_LEN:]
            else:
                aggr = aggr_full[ROUTING_AGGR_PREF_LEN:]

            if aggr not in new_ts:
                new_ts[aggr] = {}

            n_d = int(parts[2].strip() )
            n_r = int(parts[3].strip() )    
            n_a = int(parts[4].strip() )

            new_ts[aggr][round_tstamp] = (n_d, n_r, n_a)

        fp.close()


start_round_epoch = int(sys.argv[1])
end_round_epoch = int(sys.argv[2])
AGGR_PREF_LEN = len("projects.zeusping.test1.geo.netacuity.")
ROUTING_AGGR_PREF_LEN = len("projects.zeusping.test1.routing.")

new_ts = {}

build_new_ts(new_ts)

fqdn_to_oldfname_suf = {}

# loc1 is regions, loc2 is counties
regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
fp = wandio.open(regions_fname)
for line in fp:
    parts = line.strip().split(',')
    fqdn = parts[1].strip()
    st_code_full = parts[3].strip()
    st_code_parts = st_code_full.split('.')
    if len(st_code_parts) >= 2:
        fqdn_to_oldfname_suf[fqdn] = st_code_parts[1]
        
fp.close()

counties_fname = '/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz'
fp = wandio.open(counties_fname)
for line in fp:
    parts = line.strip().split(',')
    fqdn = parts[1].strip()
    county_code_parts = fqdn.split('.')
    if len(county_code_parts) >= 4:
        # print(county_code_parts)
        # sys.exit(1)
        fqdn_to_oldfname_suf[fqdn] = county_code_parts[3]
        
fp.close()

# pprint.pprint(fqdn_to_oldfname_suf['NA.US.4416.2148'])
# sys.exit(1)


for aggr in new_ts:
    # if aggr != 'Maine-11351':
    #     continue
    
    # if "asn" in aggr:
    #     continue

    sys.stderr.write("Processing aggr: {0}\n".format(aggr) )

    old_format_fname = convert_aggr_to_oldfname(aggr)

    if old_format_fname == -1:
        sys.stderr.write("Could not find a file for aggr: {0}\n".format(aggr) )
        continue

    fp = open(old_format_fname)

    for line in fp:
        parts = line.strip().split()

        this_t = int(parts[0])

        if this_t == 1617753000:
            continue

        if this_t not in new_ts[aggr]:
            sys.stderr.write("ts {0} not in aggr {1}\n".format(this_t, aggr) )
            sys.exit(1)

        this_d = new_ts[aggr][this_t]
            
        n_d = int(parts[1])
        n_r = int(parts[2])
        n_a = int(parts[3])

        if n_d == this_d[0] and n_r == this_d[1] and n_a == this_d[2]:
            sys.stdout.write("Good. ts {0} old_n_d {1} new_n_d {2} old_n_r {3} new_n_r {4} old_n_a {5} new_n_a {6}\n".format(this_t, n_d, this_d[0], n_r, this_d[1], n_a, this_d[2]) )
            pass
        else:
            sys.stdout.write("Bad. ts {0} old_n_d {1} new_n_d {2} old_n_r {3} new_n_r {4} old_n_a {5} new_n_a {6}\n".format(this_t, n_d, this_d[0], n_r, this_d[1], n_a, this_d[2]) )
            # sys.exit(1)

    
        


