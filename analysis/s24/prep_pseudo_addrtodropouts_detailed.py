
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
netacq_date = sys.argv[4]
scope = sys.argv[5]

idx_to_loc1_name = {}
idx_to_loc1_fqdn = {}
idx_to_loc1_code = {}

idx_to_loc2_name = {}
idx_to_loc2_fqdn = {}
idx_to_loc2_code = {}

if scope == 'US':
    is_US = True
    # loc1 is regions, loc2 is counties
    regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
    zeusping_helpers.load_idx_to_dicts(regions_fname, idx_to_loc1_fqdn, idx_to_loc1_name, idx_to_loc1_code, py_ver=2)
    counties_fname = '/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz'
    zeusping_helpers.load_idx_to_dicts(counties_fname, idx_to_loc2_fqdn, idx_to_loc2_name, idx_to_loc2_code, py_ver=2)
    
else:
    is_US = False
    # loc1 is countries, loc2 is regions

    ctry_code_to_fqdn = {}
    ctry_code_to_name = {}
    countries_fname = '/data/external/natural-earth/polygons/ne_10m_admin_0.countries.v3.1.0.processed.polygons.csv.gz'
    zeusping_helpers.load_idx_to_dicts(countries_fname, idx_to_loc1_fqdn, idx_to_loc1_name, idx_to_loc1_code, ctry_code_to_fqdn=ctry_code_to_fqdn, ctry_code_to_name=ctry_code_to_name, py_ver=2)
    
    regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
    zeusping_helpers.load_idx_to_dicts(regions_fname, idx_to_loc2_fqdn, idx_to_loc2_name, idx_to_loc2_code, py_ver=2)

rtree = radix.Radix()
rnode = zeusping_helpers.load_radix_tree(pfx2AS_fn, rtree)

# Load pyipmeta in order to perform geo lookups per address
provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
ipm = pyipmeta.IpMeta(provider="netacq-edge",
                      provider_config=provider_config_str)


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

        # At this point, we will obtain just the ids of loc1 and loc2. We will use other dictionaries to obtain the name and fqdn
        loc1 = 'UNKLOC1'
        loc2 = 'UNKLOC2'
        res = ipm.lookup(addr)

        if len(res) != 0:
            ctry_code = res[0]['country_code']

            if is_US is True:
                # Find US state and county
                if ctry_code != 'US':
                    continue

                loc1 = str(res[0]['polygon_ids'][1]) # This is for US state info
                loc2 = str(res[0]['polygon_ids'][0]) # This is for county info

            else:
                # Find region
                loc1 = ctry_code
                loc2 = str(res[0]['polygon_ids'][1]) # This is for region info

        ip_to_loc[addr] = [loc1, loc2]

        
ip_to_as = {}
ip_to_loc = {}
        
process_file(inp_file1)
process_file(inp_file2)

for ip in ip_to_as:
    # |1|10001|2| are just random numbers we are making up; they don't matter but we wish to be consistent with addr_to_dropouts_detailed
    if ip in ip_to_loc:
        [loc1, loc2] = ip_to_loc[ip]
    else:
        loc1 = 'UNKLOC1'
        loc2 = 'UNKLOC2'

    if is_US is True:
        sys.stdout.write("{0}|1|10001|2|{1}|{2}|{3}|{4}\n".format(ip, ip_to_as[ip], idx_to_loc1_name[loc1], loc2, idx_to_loc2_name[loc2]) )
    # sys.exit(1)
