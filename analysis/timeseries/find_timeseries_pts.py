#!/usr/bin/env python

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime

# Load pyipmeta in order to perform county lookups per address
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



def update_aggregate_details(aggregator_dict, n_r, n_n, n_d):
    aggregator_dict["resp"] += n_r
    aggregator_dict["newresp"] += n_n
    aggregator_dict["dropout"] += n_d


def write_to_file(key_to_status, fps):
    for key in key_to_status:
        if key not in fps:
            fps[key] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, key), "w")
            
        this_d = key_to_status[key]
        n_r = this_d["resp"]
        n_n = this_d["newresp"]
        n_d = this_d["dropout"]
        
        fps[key].write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )
        fps[key].flush()
        
    
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

ip_to_county = {}
inp_ips_fp = open(inp_ips_file)
sys.stderr.write("Opening inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )
for line in inp_ips_fp:
    parts = line.strip().split()
    ip = parts[0].strip()
    asn = ip_to_as[ip]

    # Find county id
    county_id = '-1'
    county_name = 'UNK'
    res = ipm.lookup(ip)
    if len(res) != 0:
        if 'polygon_ids' in res[0]:
            county_id = str(res[0]['polygon_ids'][0])
            if (county_id) in idx_to_county:
                county_name = idx_to_county[(county_id)]
                
    ip_to_county[ip] = county_id
    
    if test == 1:
        test_asn_fp.write("{0} {1} {2} {3}\n".format(line[:-1], asn, county_id, county_name) )
        
    # reqd_ips.add(ip)
    
sys.stderr.write("Done reading inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )


# for ip in ip_to_as:
#     sys.stdout.write("{0} {1}\n".format(ip, ip_to_as[ip]) )
print len(ip_to_as)

tstart = int(sys.argv[1])
tend = int(sys.argv[2])

county_asn_fps = {}
county_fps = {}
asn_fps = {}

for this_t in range(tstart, tend, 600):
    this_fname = "{0}/{1}_to_{2}".format(inp_dir, this_t, this_t + 600)
    sys.stderr.write("Processing {0} at {1}\n".format(this_fname, str(datetime.datetime.now() ) ) )    
    this_fp = open(this_fname, "r")

    # Testing code
    if test == 1:
        if this_t >= 1567027800 and this_t <= 1567114200:        
            test_fname = "{0}/{1}_to_{2}_with_county_asn".format(inp_dir, this_t, this_t + 600)
            test_fp = open(test_fname, "w")
            
    county_asn_to_status = {}
    county_to_status = {}
    asn_to_status = {}
    
    for line in this_fp:
        parts = line.strip().split()
        addr = parts[0].strip()

        status = int(parts[1].strip() )

        # Assign default value if key is not present
        asn = ip_to_as.get(addr, "UNK")
        county = ip_to_county.get(addr, "-1")

        # print line
        # print asn
        # print county
        
        if county not in county_asn_to_status:
            county_asn_to_status[county] = {}
            # print "Getting here county".format(county)
        if asn not in county_asn_to_status[county]:
            # print "Getting here asn: {0}".format(asn)
            county_asn_to_status[county][asn] = {"resp" : 0, "newresp" : 0, "dropout" : 0}

        if status == 0:
            county_asn_to_status[county][asn]["dropout"] += 1
        elif status == 1:
            county_asn_to_status[county][asn]["resp"] += 1
        elif status == 2:
            county_asn_to_status[county][asn]["newresp"] += 1

        if test == 1:
            if this_t >= 1567027800 and this_t <= 1567114200:
                test_fp.write("{0} {1} {2}\n".format(line[:-1], county, asn) )

    for county in county_asn_to_status:

        if county not in county_asn_fps:
            county_asn_fps[county] = {}
        
        for asn in county_asn_to_status[county]:

            this_d = county_asn_to_status[county][asn]
            n_r = this_d["resp"]
            n_n = this_d["newresp"]
            n_d = this_d["dropout"]

            if asn not in county_asn_fps[county]:
                county_asn_fps[county][asn] = open("{0}/resp_dropout_per_round_{1}_{2}".format(inp_dir, county, asn), "w")

            county_asn_fps[county][asn].write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )
            county_asn_fps[county][asn].flush()

            if county not in county_to_status:
                county_to_status[county] = {"resp" : 0, "newresp" : 0, "dropout" : 0}
            update_aggregate_details(county_to_status[county], n_r, n_n, n_d)

            if asn not in asn_to_status:
                asn_to_status[asn] = {"resp" : 0, "newresp" : 0, "dropout" : 0}
            update_aggregate_details(asn_to_status[asn], n_r, n_n, n_d)


    write_to_file(county_to_status, county_fps)
    # for county in county_to_status:
    #     if county not in county_fps:
    #         county_fps[county] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, county), "w")
    #     this_d = county_to_status[county]
    #     n_r = this_d["resp"]
    #     n_n = this_d["newresp"]
    #     n_d = this_d["dropout"]
        
    #     county_fps[county].write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )

    write_to_file(asn_to_status, asn_fps)
        
    if test == 1:
        if this_t >= 1567027800 and this_t <= 1567114200:        
            test_fp.close()
        
