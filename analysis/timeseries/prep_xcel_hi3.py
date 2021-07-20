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
import _pytimeseries
import pyipmeta
import wandio
from collections import defaultdict
import datetime


ts = _pytimeseries.Timeseries()

# try to get ascii by name
# print "Asking for ASCII backend by name:"
be = ts.get_backend_by_name("ascii")
# print "Got backend: %d, %s (%s)" % (be.id, be.name, be.enabled)
# print

# try to enable the ascii backend with options
# print "Enabling ASCII backend (with ignored options):"
# print ts.enable_backend(be, "ignored options")
# print be
# print

# enable the ascii backend
# print "Enabling ASCII backend:"
ts.enable_backend(be)
# print be
# print

# try to set a single value
# print "Setting a single value:"
# print "Should look like: a.test.key 12345 532051200"
# print ts.set_single("a.test.key", 12345, 532051200)
# print

kp = ts.new_keypackage(reset=True)
# print kp


idx_to_countyname = {}
countyname_to_idx = {}
# Populate county_to_id
county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
line_ct = 0 
for line in county_idxs_fp:
    line_ct += 1

    if line_ct == 1:
        continue
    
    parts = line.strip().split(',')
    idx = parts[0].strip()

    state_name_parts = parts[3].strip().split('.')
    
    if len(state_name_parts) != 3:
        continue
    
    state_name = state_name_parts[1]
    county_name = "{0}-{1}".format(state_name, parts[2][1:-1].upper() )
    countyname_to_idx[county_name] = idx
    idx_to_countyname[idx] = county_name

# print countyname_to_idx["MD-PRINCE GEORGE'S"]
# sys.exit(1)
    
inp_fname = sys.argv[1]
inp_fp = open(inp_fname, 'r')

all_tstamps = set()
county_to_custs = {}

for line in inp_fp:
    parts = line.strip().split('|')
    tstamp = int(parts[0])

    all_tstamps.add(tstamp)
    
    county_name = parts[1]
    totcusts = int(parts[2])
    outcusts = int(parts[3])

    if county_name not in county_to_custs:
        county_to_custs[county_name] = {}
    county_to_custs[county_name][tstamp] = {"totcusts" : totcusts, "outcusts" : outcusts}

inp_fp.close()


for tstamp in sorted(all_tstamps):
    for county_name in county_to_custs:

        # TODO: Hack to ensure we only get CO addresses. Change this later after changing the key appropriately (we should not be getting 4417 as the key for state that are not CO.
        if county_name[:2] != 'CO':
            continue
        
        county_idx = countyname_to_idx[county_name]        
        if tstamp in county_to_custs[county_name]:
            totcusts = county_to_custs[county_name][tstamp]["totcusts"]
            
            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.xcel_totcusts_cnt".format(county_idx)

            idx = kp.get_key(key)
            if idx is None:
                idx = kp.add_key(key)
            kp.set(idx, totcusts)

            outcusts = county_to_custs[county_name][tstamp]["outcusts"]

            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.xcel_outcusts_cnt".format(county_idx)

            idx = kp.get_key(key)
            if idx is None:
                idx = kp.add_key(key)
            kp.set(idx, outcusts)
            
    kp.flush(tstamp)    
            
