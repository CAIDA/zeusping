
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

addr_file = sys.argv[1]
netacq_date = sys.argv[2]

# Load pyipmeta in order to perform county lookups per address
provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
ipm = pyipmeta.IpMeta(provider="netacq-edge",
                      provider_config=provider_config_str)


# IODA uses a mapping of an integer-ID to a county-name. Load that mapping. We will use this mapping to obtain the county-name associated with the integer-ID when annotating IP addresses
idx_to_county = {}
# Populate idx_to_county
county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
for line in county_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    county_name = parts[2][1:-1] # Get rid of quotes
    idx_to_county[idx] = county_name

    
# Obtain mapping between region-ID to a region-name.
idx_to_region = {}
region_idxs_fp = wandio.open('/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz')
for line in region_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    fqdn = parts[1].strip()
    region_name = parts[2][1:-1] # Get rid of quotes
    idx_to_region[idx] = region_name


fp = open(addr_file)
for line in fp:
    addr = line[:-1]
    
    # Find loc id
    loc_id = '-1'
    loc_name = 'UNK'
    res = ipm.lookup(addr)
    if len(res) != 0:
        if 'polygon_ids' in res[0]:
            # print res

            county_id = str(res[0]['polygon_ids'][0]) # This is for county info
            county_name = "UNDEF"
            if county_id in idx_to_county:
                county_name = idx_to_county[county_id]

            region_name = "UNDEF"
            region_id = str(res[0]['polygon_ids'][1]) # This is for region info
            if region_id in idx_to_region:
                region_name = idx_to_region[region_id]
                
    sys.stdout.write("{0} {1} {2} {3} {4} {5} {6} {7}\n".format(addr, res[0]['continent_code'], res[0]['country_code'], region_id, region_name, res[0]['city'], county_id, county_name) )
    # sys.exit(1)
