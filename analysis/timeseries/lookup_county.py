#!/usr/bin/env python

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
