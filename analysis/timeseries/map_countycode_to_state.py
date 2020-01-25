#!/usr/bin/env python

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime

idx_to_county = {}
idx_to_fqdn = {}
# Populate county_to_id
county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
for line in county_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    fqdn = parts[1].strip()
    county_name = parts[2][1:-1] # Get rid of quotes
    idx_to_county[idx] = county_name
    idx_to_fqdn[idx] = fqdn

for idx in idx_to_fqdn:
    print idx, idx_to_fqdn[idx]





