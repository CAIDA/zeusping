#!/usr/bin/env python

import sys
# import pyipmeta
from collections import namedtuple
# import wandio
import datetime

idx_to_region = {}
idx_to_fqdn = {}

# Populate region_to_id
# region_idxs_fp = wandio.open('/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz')
# for line in region_idxs_fp:
for line in sys.stdin:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    fqdn = parts[1].strip()
    region_name = parts[2][1:-1] # Get rid of quotes
    idx_to_region[idx] = region_name
    idx_to_fqdn[idx] = fqdn

for idx in idx_to_fqdn:
    print (idx, idx_to_fqdn[idx], idx_to_region[idx])





