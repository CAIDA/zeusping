#!/usr/bin/env python

# TODO: Be consistent with wandio
# NOTE: I've changed the output format of addr_to_dropouts_detailed, so whichever files use it need to be modified appropriately
# NOTE: Let us not update asn_to_status anymore. Since we are probing many states and the same AS may repeat acorss states, let the Explorer compose aggregate AS statistics. We will stick to updating state-ASN and county-ASN statistics.
# TODO: Experiment without flush(). See how much our efficiency improves.
# TODO: Also change find_timeseries_pts_per_ctry.py to reflect these changes
# TODO: Modularize this code and make it nicer
# TODO: Have the state name be part of resp_dropout_per_round_{0}.format(county)

# First, we get idx_to_county
# Next, we try to populate which locations and which ASNs we need in each location. This helps us to populate ip_to_as using existing ip_to_as files but without using too much memory (we don't need ip_to_as for non-residential ASNs in those locs, such as Google in CA).
# usstate_to_reqd_asns is a set(). usstate_to_reqd_asns['CA'] = set(22773, 7922, 20001)
# We will then get ip_to_as, ip_to_county, ip_to_state.
# Read each 10-minute round file containing dropout, resp, antidropouts. For each IP, find state, county, asn. Then update the dict for each of these aggregates.

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime


def update_aggregate_details(aggregator_dict, n_r, n_n, n_d):
    aggregator_dict["resp"] += n_r
    aggregator_dict["antidropout"] += n_n
    aggregator_dict["dropout"] += n_d


def write_to_file(this_t, key_to_status, fps, isasn = False):
    for key in key_to_status:
        if key not in fps:
            if isasn == False:
                if must_append == 0:
                    if IS_OUTPUT_COMPRESSED == 0:
                        fps[key] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, key), "w")
                    else:
                        fps[key] = wandio.open("{0}/resp_dropout_per_round_{1}.gz".format(inp_dir, key), "w")
                else:
                    if IS_OUTPUT_COMPRESSED == 0:
                        fps[key] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, key), "a")
                    else:
                        # NOTE: Looks like wandio does not support open in 'append' mode
                        fps[key] = wandio.open("{0}/resp_dropout_per_round_{1}.gz".format(inp_dir, key), "a")
            else:
                # fps[key] = wandio.open("{0}/resp_dropout_per_round_AS{1}.gz".format(inp_dir, key), "w")
                if must_append == 0:
                    if IS_OUTPUT_COMPRESSED == 0:
                        fps[key] = open("{0}/resp_dropout_per_round_AS{1}".format(inp_dir, key), "w")
                    else:
                        fps[key] = wandio.open("{0}/resp_dropout_per_round_AS{1}.gz".format(inp_dir, key), "w")
                else:
                    if IS_OUTPUT_COMPRESSED == 0:
                        fps[key] = open("{0}/resp_dropout_per_round_AS{1}".format(inp_dir, key), "a")
                    else:
                        # NOTE: Looks like wandio does not support open in 'append' mode
                        fps[key] = wandio.open("{0}/resp_dropout_per_round_AS{1}.gz".format(inp_dir, key), "a")
                    
        this_d = key_to_status[key]
        n_r = this_d["resp"]
        n_n = this_d["antidropout"]
        n_d = this_d["dropout"]
        
        fps[key].write("{0} {1} {2} {3}\n".format(this_t, n_d, n_r, n_n) )
        # fps[key].flush()
        
    
test = 1
IS_INPUT_COMPRESSED = 1
IS_OUTPUT_COMPRESSED = 0
is_old_CO = 0 # For processing our first experiment on CO

tstart = int(sys.argv[1])
tend = int(sys.argv[2])
inp_dir = sys.argv[3]
netacq_date = sys.argv[4]
usstate_to_reqd_asns_fname = sys.argv[5] # NOTE: Even if we're handling old CO, note that we'll need a usstate_to_reqd_asns_fname.
if (len(sys.argv) > 6):
   if sys.argv[6] == 'append': # If we mess up processing and want to append to files instead of overwriting files
       must_append = 1
   else:
       must_append = 0
else:
    must_append = 0


# Load pyipmeta in order to perform county lookups per address
provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
ipm = pyipmeta.IpMeta(provider="netacq-edge",
                      provider_config=provider_config_str)


# IODA uses a mapping of an integer to a county-name. Load that mapping. We use this mapping to produce an "annotated" version of the addr_to_dropouts file
idx_to_county = {}
# Populate idx_to_county
county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
for line in county_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    county_name = parts[2][1:-1] # Get rid of quotes
    idx_to_county[idx] = county_name


# Let's get ip to as mappings for the IP addresses that we pinged in each U.S. state
# loc_to_reqd_asns = {
#     "CO" : {'7922', '209', '7155'},
#     "VT" : {'7922', '13977'},
#     # "RI" : {'22773', '701', '7029', '7922'},
#     "RI" : {'22773', '701'},    
#     "CA" : {'7922', '7155'},
#     "accra" : {'30986', '37140'},
#     }

usstate_to_reqd_asns = {}
if is_old_CO == 1:
    # TODO: Handle old_CO. Use Git to find how I did this back in the day.
    pass
else:

    # TODO: Replace this unmodularized code with the populate_usstate_to_reqd_asns function from quick_find_resp_addrs.py
    usstate_to_reqd_asns_fp = open(usstate_to_reqd_asns_fname)

    for line in usstate_to_reqd_asns_fp:
        parts = line.strip().split()
        usstate = parts[0].strip()

        if usstate not in usstate_to_reqd_asns:
            usstate_to_reqd_asns[usstate] = set()

        asn_list = parts[1].strip()
        asns = asn_list.strip().split('-')

        for asn in asns:
            asns_reqd_splits_parts = asn.strip().split(':')
            usstate_to_reqd_asns[usstate].add(asns_reqd_splits_parts[0])

print usstate_to_reqd_asns
# sys.exit(1)

ip_to_as = {}
ip_to_usstate = {}
for usstate in usstate_to_reqd_asns:

    usstate_ip_to_as_file = '/scratch/zeusping/probelists/us_addrs/{0}_addrs/all_{0}_addresses_20200805.pfx2as.gz'.format(usstate)
    
    ip_to_as_fp = wandio.open(usstate_ip_to_as_file)
    sys.stderr.write("Opening ip_to_as_fp for {0} at {1}\n".format(usstate, str(datetime.datetime.now() ) ) )
    line_ct = 0
    for line in ip_to_as_fp:

        line_ct += 1

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} ip_to_as lines for {1} read at {2}\n".format(line_ct, usstate, str(datetime.datetime.now() ) ) )

        parts = line.strip().split('|')

        if (len(parts) != 2):
            continue

        # addr = parts[0].strip()
        asn = parts[1].strip()

        # if ( (asn == '7922') or (asn == '209') or (asn == '11351') or (asn == '10796') or (asn == '20001') or (asn == '7843') or (asn == '11426') or (asn == '33588') or (asn == '12271') ): # Colorado        
        if asn in usstate_to_reqd_asns[usstate]:
            addr = parts[0].strip()

            if is_old_CO == 1:
                # These lines were for the old CO, where Charter used tons of ASNs.
                # UPDATE: I'm not sure what benefit we obtain by having all Charter be 20001. There could be differences in response-rates among different Charter ASNs. So let each Charter AS be its own AS instead of mapping them all to 20001
                # if asn != '7922' and asn != '209':
                #     asn = '20001'
                pass

            ip_to_as[addr] = asn
            ip_to_usstate[addr] = usstate

    sys.stderr.write("Done reading ip_to_as for {0} at {1}\n".format(usstate, str(datetime.datetime.now() ) ) )


# reqd_ips = set()

# Let's get ip to county mappings for the IP addresses that we pinged in each U.S. state    
ip_to_county = {}

# Let's also create an annotated filename which contains the USstate, county and AS mapping for each IP address
# annotated_fname = "{0}/addr_to_dropouts_detailed.gz".format(inp_dir)
annotated_fname = "{0}/addr_to_dropouts_detailed".format(inp_dir)    
# annotated_fp = wandio.open(annotated_fname, 'w')
annotated_fp = open(annotated_fname, 'w')    

if IS_INPUT_COMPRESSED == 0:
    inp_ips_file = "{0}/addr_to_dropouts".format(inp_dir)
    inp_ips_fp = open(inp_ips_file)
else:
    inp_ips_file = "{0}/addr_to_dropouts.gz".format(inp_dir)
    inp_ips_fp = wandio.open(inp_ips_file)

sys.stderr.write("Opening inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )
for line in inp_ips_fp:
    parts = line.strip().split()
    ip = parts[0].strip()

    # if ip in ip_to_usstate:
    #     usstate = ip_to_usstate[ip]
    # else:
    #     usstate = '-1'
    usstate = ip_to_usstate.get(ip, "-1")
        
    # if ip in ip_to_as:
    #     asn = ip_to_as[ip]
    # else:
    #     asn = '-1'
    asn = ip_to_as.get(ip, "UNK")

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
    
    annotated_fp.write("{0} {1} {2} {3} {4}\n".format(line[:-1], usstate, asn, county_id, county_name) )
        
    # reqd_ips.add(ip)
    
sys.stderr.write("Done reading inp_ips_fp at {0}\n".format(str(datetime.datetime.now() ) ) )


# Populate time series points for various aggregates

usstate_asn_fps = {}
usstate_fps = {}
county_asn_fps = {}
county_fps = {}
asn_fps = {}

test_tstart = 1598353200 # Tue Aug 25 11:00:00 UTC 2020
test_tend = 1598439600 # Wed Aug 26 11:00:00 UTC 2020

for this_t in range(tstart, tend, 600):
    # this_fname = "{0}/{1}_to_{2}.gz".format(inp_dir, this_t, this_t + 600)
    if is_old_CO == 1:
        this_fname = "{0}/{1}_to_{2}/dropout_resp_antidropout_addrs.gz".format(inp_dir, this_t, this_t + 600)
    else:
        if IS_INPUT_COMPRESSED == 1:
            this_fname = "{0}/{1}_to_{2}.gz".format(inp_dir, this_t, this_t + 600)
        else:
            this_fname = "{0}/{1}_to_{2}".format(inp_dir, this_t, this_t + 600)
    sys.stderr.write("Processing {0} at {1}\n".format(this_fname, str(datetime.datetime.now() ) ) )

    try:
        if is_old_CO == 1:
            this_fp = wandio.open(this_fname, "r")
        else:
            if IS_INPUT_COMPRESSED == 1:
                this_fp = wandio.open(this_fname, "r")
            else:
                this_fp = open(this_fname, "r")
    except:
        # Sometimes, we have missing data for some 10-minute rounds. Handle it.
        continue

    # Testing code
    if test == 1:
        if this_t >= test_tstart and this_t <= test_tend:
            test_fname = "{0}/{1}_to_{2}_with_county_asn".format(inp_dir, this_t, this_t + 600)
            test_fp = open(test_fname, "w")

    usstate_asn_to_status = {}
    usstate_to_status = {}
    county_asn_to_status = {}
    county_to_status = {}
    asn_to_status = {}
    
    for line in this_fp:
        parts = line.strip().split()
        addr = parts[0].strip()

        status = int(parts[1].strip() )

        # Assign default value if key is not present
        usstate = ip_to_usstate.get(addr, "-1")
        asn = ip_to_as.get(addr, "UNK")
        county = ip_to_county.get(addr, "-1")

        # print line
        # print usstate
        # print asn
        # print county

        if usstate not in usstate_asn_to_status:
            usstate_asn_to_status[usstate] = {}
            # print "Getting here usstate".format(usstate)
        if asn not in usstate_asn_to_status[usstate]:
            # print "Getting here asn: {0}".format(asn)
            usstate_asn_to_status[usstate][asn] = {"resp" : 0, "antidropout" : 0, "dropout" : 0}
        
        if county not in county_asn_to_status:
            county_asn_to_status[county] = {}
            # print "Getting here county".format(county)
        if asn not in county_asn_to_status[county]:
            # print "Getting here asn: {0}".format(asn)
            county_asn_to_status[county][asn] = {"resp" : 0, "antidropout" : 0, "dropout" : 0}

        if status == 0:
            usstate_asn_to_status[usstate][asn]["dropout"] += 1
            county_asn_to_status[county][asn]["dropout"] += 1
        elif status == 1:
            usstate_asn_to_status[usstate][asn]["resp"] += 1
            county_asn_to_status[county][asn]["resp"] += 1
        elif status == 2:
            usstate_asn_to_status[usstate][asn]["antidropout"] += 1
            county_asn_to_status[county][asn]["antidropout"] += 1

        if test == 1:
            if this_t >= test_tstart and this_t <= test_tend:            
                test_fp.write("{0} {1} {2} {3}\n".format(line[:-1], usstate, county, asn) )


    for usstate in usstate_asn_to_status:
        if usstate not in usstate_asn_fps:
            usstate_asn_fps[usstate] = {}

        for asn in usstate_asn_to_status[usstate]:

            this_d = usstate_asn_to_status[usstate][asn]
            n_r = this_d["resp"]
            n_n = this_d["antidropout"]
            n_d = this_d["dropout"]

            if asn not in usstate_asn_fps[usstate]:
                if must_append == 0:
                    usstate_asn_fps[usstate][asn] = open("{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, usstate, asn), "w")
                else:
                    usstate_asn_fps[usstate][asn] = open("{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, usstate, asn), "a")

            usstate_asn_fps[usstate][asn].write("{0} {1} {2} {3}\n".format(this_t, n_d, n_r, n_n) )
            # usstate_asn_fps[usstate][asn].flush()

            if usstate not in usstate_to_status:
                usstate_to_status[usstate] = {"resp" : 0, "antidropout" : 0, "dropout" : 0}
            update_aggregate_details(usstate_to_status[usstate], n_r, n_n, n_d)

            # NOTE: Let us not update asn_to_status. See comment at the start of the file
            # if asn not in asn_to_status:
            #     asn_to_status[asn] = {"resp" : 0, "antidropout" : 0, "dropout" : 0}
            # update_aggregate_details(asn_to_status[asn], n_r, n_n, n_d)

    write_to_file(this_t, usstate_to_status, usstate_fps)
    # write_to_file(this_t, asn_to_status, asn_fps, isasn=True)
                
    for county in county_asn_to_status:

        if county not in county_asn_fps:
            county_asn_fps[county] = {}
        
        for asn in county_asn_to_status[county]:

            this_d = county_asn_to_status[county][asn]
            n_r = this_d["resp"]
            n_n = this_d["antidropout"]
            n_d = this_d["dropout"]

            if asn not in county_asn_fps[county]:
                # county_asn_fps[county][asn] = wandio.open("{0}/resp_dropout_per_round_{1}_AS{2}.gz".format(inp_dir, county, asn), "w")
                if must_append == 0:
                    county_asn_fps[county][asn] = open("{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, county, asn), "w")
                else:
                    county_asn_fps[county][asn] = open("{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, county, asn), "a")                    

            county_asn_fps[county][asn].write("{0} {1} {2} {3}\n".format(this_t, n_d, n_r, n_n) )
            # county_asn_fps[county][asn].flush()

            if county not in county_to_status:
                county_to_status[county] = {"resp" : 0, "antidropout" : 0, "dropout" : 0}
            update_aggregate_details(county_to_status[county], n_r, n_n, n_d)

            
    write_to_file(this_t, county_to_status, county_fps)
    # for county in county_to_status:
    #     if county not in county_fps:
    #         county_fps[county] = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, county), "w")
    #     this_d = county_to_status[county]
    #     n_r = this_d["resp"]
    #     n_n = this_d["antidropout"]
    #     n_d = this_d["dropout"]
        
    #     county_fps[county].write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )
        
    if test == 1:
        if this_t >= test_tstart and this_t <= test_tend:                    
            test_fp.close()
        
