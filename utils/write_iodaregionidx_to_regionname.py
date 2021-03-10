
import sys
from collections import namedtuple
import wandio
import datetime

# Obtain mapping between region-ID to a region-name.
region_idxs_fp = wandio.open('/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz')
line_ct = 0
for line in region_idxs_fp:

    line_ct += 1

    if line_ct == 1:
        continue
    
    parts = line.strip().split(',')
    idx = parts[0].strip()
    fqdn = parts[1].strip()
    region_name = parts[2][1:-1] # Get rid of quotes

    sys.stdout.write("{0},{1}\n".format(idx, region_name) )

