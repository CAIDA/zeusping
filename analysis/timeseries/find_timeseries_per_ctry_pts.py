#!/usr/bin/env python3

import sys
import pyipmeta
from collections import namedtuple
import datetime


def populate_ip_to_as(ctry_ip_to_as_file, ip_to_as):
    
    ctry_ip_to_as_fp = open(ctry_ip_to_as_file)
    for line in ctry_ip_to_as_fp:

        parts = line.strip().split()
        ctry = parts[0]
        ip_to_as_file = parts[1]

        if IS_INPUT_COMPRESSED == 1:
            ip_to_as_fp = wandio.open(ip_to_as_file)
        else:
            ip_to_as_fp = open(ip_to_as_file)

        sys.stderr.write("Opening ip_to_as_fp for {0} at {1}\n".format(ctry, str(datetime.datetime.now() ) ) )
        line_ct = 0

        for line in ip_to_as_fp:

            line_ct += 1

            if line_ct%1000000 == 0:
                sys.stderr.write("{0} ip_to_as lines for {1} read at {2}\n".format(line_ct, ctry, str(datetime.datetime.now() ) ) )

            parts = line.strip().split('|')

            if (len(parts) != 2):
                continue

            # addr = parts[0].strip()
            asn = parts[1].strip()

            addr = parts[0].strip()

            ip_to_as[addr] = asn

        sys.stderr.write("Done reading ip_to_as for {0} at {1}\n".format(ctry, str(datetime.datetime.now() ) ) )

        
def update_aggregate_details(aggregator_dict, n_r, n_n, n_d):
    aggregator_dict["resp"] += n_r
    aggregator_dict["newresp"] += n_n
    aggregator_dict["dropout"] += n_d


def write_to_file(key_to_status, fps, isasn = False):
    for key in key_to_status:
        if key not in fps:
            if isasn == False:
                # fps[key] = wandio.open("{0}/resp_dropout_per_round_{1}.gz".format(inp_dir, key), "w")
                if must_append == 0:
                    fps[key] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, key), "w")
                else:
                    fps[key] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, key), "a")
            else:
                # fps[key] = wandio.open("{0}/resp_dropout_per_round_AS{1}.gz".format(inp_dir, key), "w")
                if must_append == 0:
                    fps[key] = open("{0}/resp_dropout_per_round_AS{1}".format(inp_dir, key), "w")
                else:
                    fps[key] = open("{0}/resp_dropout_per_round_AS{1}".format(inp_dir, key), "a")
                    
        this_d = key_to_status[key]
        n_r = this_d["resp"]
        n_n = this_d["newresp"]
        n_d = this_d["dropout"]
        
        fps[key].write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )
        # fps[key].flush()
        


# Global variables
test = 0
py_ver = 3
if py_ver == 3:
    # wandio does not have a Python3 module. So if we're using Python3, we have to assume that the input files are uncompressed.
    IS_INPUT_COMPRESSED = 0
elif py_ver == 2:
    IS_INPUT_COMPRESSED = 1

tstart = int(sys.argv[1])
tend = int(sys.argv[2])
inp_dir = sys.argv[3]
netacq_date = sys.argv[4]
ctry_ip_to_as_file = sys.argv[5] # Each line of this file is a path to an AS file for a U.S. state

if (len(sys.argv) > 6):
   if sys.argv[6] == 'append': # If we mess up processing and want to append to files instead of overwriting files
       must_append = 1
   else:
       must_append = 0
else:
    must_append = 0
    
if py_ver == 2:
    import wandio
    
# Load pyipmeta in order to perform region lookups per address
provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
ipm = pyipmeta.IpMeta(provider="netacq-edge",
                      provider_config=provider_config_str)

# Obtain mapping between region/county (i.e. loc) idx to loc
idx_to_loc = {}
# region_idxs_fp = wandio.open('/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz')
# for line in region_idxs_fp:
for line in sys.stdin:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    fqdn = parts[1].strip()
    region_name = parts[2][1:-1] # Get rid of quotes
    idx_to_loc[idx] = region_name
    # idx_to_fqdn[idx] = fqdn


ip_to_as = {}
populate_ip_to_as(ctry_ip_to_as_file, ip_to_as)
# reqd_ips = set()

if test == 1:
    # test_asn_fname = "{0}/addr_to_dropouts_detailed.gz".format(inp_dir)
    test_asn_fname = "{0}/addr_to_dropouts_detailed".format(inp_dir)    
    # test_asn_fp = wandio.open(test_asn_fname, 'w')
    test_asn_fp = open(test_asn_fname, 'w')    


# Let's get ip to region mappings for the IP addresses that we pinged in each country
ip_to_loc = {}
if IS_INPUT_COMPRESSED == 1:
    inp_ips_file = "{0}/addr_to_dropouts.gz".format(inp_dir)    
    inp_ips_fp = wandio.open(inp_ips_file)
else:
    inp_ips_file = "{0}/addr_to_dropouts".format(inp_dir)    
    inp_ips_fp = open(inp_ips_file)
    
sys.stderr.write("Opening inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )
for line in inp_ips_fp:
    parts = line.strip().split()
    ip = parts[0].strip()
    asn = ip_to_as[ip]

    # Find loc id
    loc_id = '-1'
    loc_name = 'UNK'
    res = ipm.lookup(ip)
    if len(res) != 0:
        if 'polygon_ids' in res[0]:
            # loc_id = str(res[0]['polygon_ids'][0]) # This is for county info
            loc_id = str(res[0]['polygon_ids'][1]) # This is for region info
            if (loc_id) in idx_to_loc:
                loc_name = idx_to_loc[(loc_id)]
                
    ip_to_loc[ip] = loc_id
    
    if test == 1:
        test_asn_fp.write("{0} {1} {2} {3}\n".format(line[:-1], asn, loc_id, loc_name) )
        
    # reqd_ips.add(ip)
    
sys.stderr.write("Done reading inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )


# Populate time series points for various aggregates

loc_asn_fps = {}
loc_fps = {}
asn_fps = {}

test_tstart = 1583020800 # Mar  1 00:00:00 UTC 2020
test_tend = 1583107200 # Mar  2 00:00:00 UTC 2020

for this_t in range(tstart, tend, 600):

    if IS_INPUT_COMPRESSED == 1:
        this_fname = "{0}/{1}_to_{2}.gz".format(inp_dir, this_t, this_t + 600)
    else:
        this_fname = "{0}/{1}_to_{2}".format(inp_dir, this_t, this_t + 600)
        
    sys.stderr.write("Processing {0} at {1}\n".format(this_fname, str(datetime.datetime.now() ) ) )

    try:
        if IS_INPUT_COMPRESSED == 1:
            this_fp = wandio.open(this_fname, "r")
        else:
            this_fp = open(this_fname, "r")
    except IOError:            
        # Sometimes, we have missing data for some 10-minute rounds. Handle it.
        continue

    # Testing code
    if test == 1:
        if this_t >= test_tstart and this_t <= test_tend:
            test_fname = "{0}/{1}_to_{2}_with_loc_asn".format(inp_dir, this_t, this_t + 600)
            test_fp = open(test_fname, "w")
            
    loc_asn_to_status = {}
    loc_to_status = {}
    asn_to_status = {}
    
    for line in this_fp:
        parts = line.strip().split()
        addr = parts[0].strip()

        status = int(parts[1].strip() )

        # Assign default value if key is not present
        asn = ip_to_as.get(addr, "UNK")
        loc = ip_to_loc.get(addr, "-1")

        # print line
        # print asn
        # print loc
        
        if loc not in loc_asn_to_status:
            loc_asn_to_status[loc] = {}
            # print "Getting here loc".format(loc)
        if asn not in loc_asn_to_status[loc]:
            # print "Getting here asn: {0}".format(asn)
            loc_asn_to_status[loc][asn] = {"resp" : 0, "newresp" : 0, "dropout" : 0}

        if status == 0:
            loc_asn_to_status[loc][asn]["dropout"] += 1
        elif status == 1:
            loc_asn_to_status[loc][asn]["resp"] += 1
        elif status == 2:
            loc_asn_to_status[loc][asn]["newresp"] += 1

        if test == 1:
            if this_t >= test_tstart and this_t <= test_tend:            
                test_fp.write("{0} {1} {2}\n".format(line[:-1], loc, asn) )

    for loc in loc_asn_to_status:

        if loc not in loc_asn_fps:
            loc_asn_fps[loc] = {}
        
        for asn in loc_asn_to_status[loc]:

            this_d = loc_asn_to_status[loc][asn]
            n_r = this_d["resp"]
            n_n = this_d["newresp"]
            n_d = this_d["dropout"]

            if asn not in loc_asn_fps[loc]:
                # loc_asn_fps[loc][asn] = wandio.open("{0}/resp_dropout_per_round_{1}_AS{2}.gz".format(inp_dir, loc, asn), "w")
                if must_append == 0:
                    loc_asn_fps[loc][asn] = open("{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, loc, asn), "w")
                else:
                    loc_asn_fps[loc][asn] = open("{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, loc, asn), "a")                    
                
            loc_asn_fps[loc][asn].write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )
            # loc_asn_fps[loc][asn].flush()

            if loc not in loc_to_status:
                loc_to_status[loc] = {"resp" : 0, "newresp" : 0, "dropout" : 0}
            update_aggregate_details(loc_to_status[loc], n_r, n_n, n_d)

            if asn not in asn_to_status:
                asn_to_status[asn] = {"resp" : 0, "newresp" : 0, "dropout" : 0}
            update_aggregate_details(asn_to_status[asn], n_r, n_n, n_d)


    write_to_file(loc_to_status, loc_fps)

    write_to_file(asn_to_status, asn_fps, isasn=True)
        
    if test == 1:
        if this_t >= test_tstart and this_t <= test_tend:                    
            test_fp.close()
        
