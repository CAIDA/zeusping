#!/usr/bin/env python

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime

# This is for eventual lookups
# ipm = pyipmeta.IpMeta(provider="netacq-edge",
#                       provider_config="-b /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz")

# ipm = pyipmeta.IpMeta(provider="netacq-edge",
#                       provider_config=" -b /data/external/netacuity-dumps/Edge-processed/netacq-4-blocks.latest.csv.gz -l /data/external/netacuity-dumps/Edge-processed/netacq-4-locations.latest.csv.gz -p /data/external/netacuity-dumps/Edge-processed/netacq-4-polygons.latest.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz")

# ipm.lookup("192.172.226.97")
# print("")


ip_to_as = {}
# ip_to_as_file = sys.argv[2]
# ip_to_as_fp = wandio.open(ip_to_as_file)
# sys.stderr.write("Opening ip_to_as_fp at {0}\n".format(str(datetime.datetime.now() ) ) )
line_ct = 0
for line in sys.stdin:
# for line in ip_to_as_fp:

    line_ct += 1

    if line_ct%1000000 == 0:
        sys.stderr.write("{0} lines read at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )
    
    parts = line.strip().split('|')
    
    if (len(parts) != 2):
        continue
    
    # addr = parts[0].strip()
    asn = parts[1].strip()

    if ( (asn == '7922') or (asn == '209') or (asn == '11351') or (asn == '10796') or (asn == '20001') or (asn == '7843') or (asn == '11426') or (asn == '33588') or (asn == '12271') ): # Colorado
        addr = parts[0].strip()

        if asn != '7922' and asn != '209':
            asn = '20001'
        
        ip_to_as[addr] = asn

sys.stderr.write("Done reading ip_to_as_fp at {0}\n".format(str(datetime.datetime.now() ) ) )

test = 1

inp_dir = sys.argv[3]

# reqd_ips = set()

inp_ips_file = "{0}/addr_to_dropouts".format(inp_dir)
if test == 1:
    test_asn_fname = "{0}/addr_to_dropouts_detailed".format(inp_dir)
    test_asn_fp = open(test_asn_fname, 'w')
inp_ips_fp = open(inp_ips_file)
sys.stderr.write("Opening inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )
for line in inp_ips_fp:
    parts = line.strip().split()
    ip = parts[0].strip()
    asn = ip_to_as[ip]
    
    if test == 1:
        test_asn_fp.write("{0} {1}\n".format(line[:-1], asn) )
        
    # reqd_ips.add(ip)
    
sys.stderr.write("Done reading inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )


# for ip in ip_to_as:
#     sys.stdout.write("{0} {1}\n".format(ip, ip_to_as[ip]) )
print len(ip_to_as)

tstart = int(sys.argv[1])
tend = int(sys.argv[2])

resp_dropout_per_round_fps = {}

for this_t in range(tstart, tend, 600):
    this_fname = "{0}/{1}_to_{2}".format(inp_dir, this_t, this_t + 600)
    sys.stderr.write("Processing {0} at {1}\n".format(this_fname, str(datetime.datetime.now() ) ) )    
    this_fp = open(this_fname, "r")

    # Testing code
    if test == 1:
        if this_t >= 1565818200:
            test_fname = "{0}/{1}_to_{2}_with_asn".format(inp_dir, this_t, this_t + 600)
            test_fp = open(test_fname, "w")
            
    key_to_status = {}
    # asn_to_status = {
    #     '7922' : [0, 0, 0],
    #     '209' : [0, 0, 0],
    #     '20001' : [0, 0, 0]
    # }
    
    for line in this_fp:
        parts = line.strip().split()
        addr = parts[0].strip()

        # if addr not in ip_to_as:
            
        #     continue
        
        status = int(parts[1].strip() )

        asn = ip_to_as[addr]

        if asn not in key_to_status:
            key_to_status[asn] = {"resp" : 0, "newresp" : 0, "dropout" : 0}

        if status == 0:
            key_to_status[asn]["dropout"] += 1
        elif status == 1:
            key_to_status[asn]["resp"] += 1
        elif status == 2:
            key_to_status[asn]["newresp"] += 1

        if test == 1:
            if this_t >= 1565818200:
                test_fp.write("{0} {1}\n".format(line[:-1], asn) )
            

    for key in key_to_status:
        if key not in resp_dropout_per_round_fps:
            resp_dropout_per_round_fps[key] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, key), "w")

        n_r = key_to_status[key]["resp"]
        n_n = key_to_status[key]["newresp"]
        n_d = key_to_status[key]["dropout"]
            
        resp_dropout_per_round_fps[key].write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )
        resp_dropout_per_round_fps[key].flush()
        
    if test == 1:
        if this_t >= 1565818200:
            test_fp.close()
        
