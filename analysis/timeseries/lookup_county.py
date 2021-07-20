#!/usr/bin/env python

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

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime

# This is for county lookups
ipm = pyipmeta.IpMeta(provider="netacq-edge",
                      provider_config="-b /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz")


idx_to_county = {}
# Populate county_to_id
county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
for line in county_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    county_name = parts[2][1:-1] # Get rid of quotes
    idx_to_county[idx] = county_name


addrs = ["192.172.226.97", "24.43.127.107", "73.81.113.43", "165.123.224.104", "63.158.47.177", "70.95.162.90", "67.190.152.200", "184.96.9.144"]

for addr in addrs:
    res = ipm.lookup(addr)
    if len(res) != 0:
        print res
        if 'polygon_ids' in res[0]:
            county_id = res[0]['polygon_ids'][0]
            state_id = res[0]['polygon_ids'][1]
            print county_id
            print idx_to_county[str(county_id)]
# print("")
