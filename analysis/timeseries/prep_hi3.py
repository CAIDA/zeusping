#!/usr/bin/env python

import sys
import _pytimeseries
import pyipmeta
# import wandio
from collections import defaultdict
import datetime

# Load pyipmeta in order to perform county lookups per address
# ipm = pyipmeta.IpMeta(provider="netacq-edge",
#                       provider_config="-b /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz")

IS_INPUT_COMPRESSED = 0

# Get idx_to_fqdn for all counties
idx_to_county = {}
idx_to_fqdn = {}
if IS_INPUT_COMPRESSED == 1:
    county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
else:
    county_idxs_fp = open('gadm.counties.v2.0.processed.polygons.csv')
for line in county_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    fqdn = parts[1].strip()
    county_name = parts[2][1:-1] # Get rid of quotes
    idx_to_county[idx] = county_name
    idx_to_fqdn[idx] = fqdn

    
def populate_idx_to_val(this_d, list_of_keys, county_asn=False, county_idx='None'):

    for this_k in list_of_keys:

        if county_asn == True:
            if IS_INPUT_COMPRESSED == 1:
                inp_fname = "{0}/resp_dropout_per_round_{1}_AS{2}.gz".format(inp_dir, county_idx, this_k)
            else:
                inp_fname = "{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, county_idx, this_k)
        elif 'asns' in mode:
            # The following is a temporary hack because I did not distinguish between AS209 and the county 209. I have since prepended the AS files with _AS<ASnum> to prevent this ambiguity.
            # if 'counties' in mode and this_k == '209':
            #     continue
            if IS_INPUT_COMPRESSED == 1:
                inp_fname = "{0}/resp_dropout_per_round_AS{1}.gz".format(inp_dir, this_k)
            else:
                inp_fname = "{0}/resp_dropout_per_round_AS{1}".format(inp_dir, this_k)
        else:
            if IS_INPUT_COMPRESSED == 1:
                inp_fname = "{0}/resp_dropout_per_round_{1}.gz".format(inp_dir, this_k)
            else:
                inp_fname = "{0}/resp_dropout_per_round_{1}".format(inp_dir, this_k)
            
        try:
            # TODO: wandio.open
            if IS_INPUT_COMPRESSED == 1:
                inp_fp = wandio.open(inp_fname, "r")
            else:
                inp_fp = open(inp_fname, "r")

        except IOError:
            # sys.stderr.write("{0} could not be opened\n".format(inp_fname) )
            continue

        for line in inp_fp:
            parts = line.strip().split()
            tstamp = int(parts[0].strip() )
            n_d = int(parts[1].strip() )
            n_r = int(parts[2].strip() )
            n_a = int(parts[3].strip() )

            all_tstamps.add(tstamp)

            if this_k not in this_d:
                this_d[this_k] = {"n_d" : {}, "n_r" : {}, "n_a" : {} }
            this_d[this_k]["n_d"][tstamp] = n_d
            this_d[this_k]["n_r"][tstamp] = n_r
            this_d[this_k]["n_a"][tstamp] = n_a            


def set_keys_for_this_tstamp(this_d, list_of_keys, tstamp, mode, county_idx=None):
    for this_k in this_d:

        # We have some missing data at 1566523200. So some keys do not have a corresponding entry
        # IMP: Turns out that the keypackage automatically sets keys to 0 if we do not specify them. So even for county_idx 2130, 2011, and 1939, the values are set to 0 for tstamp 1566523200. (Check test_kp.py)
        if tstamp not in this_d[this_k]["n_r"]:
            # sys.stderr.write("No tstamp {0} for key {1}\n".format(tstamp, this_k) )
            continue

        n_d = this_d[this_k]["n_d"][tstamp]
        n_r = this_d[this_k]["n_r"][tstamp]
        n_a = this_d[this_k]["n_a"][tstamp]

        if 'county-asn' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.dropout_addr_cnt".format(county_idx, this_k)
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.dropout_addr_cnt".format(idx_to_fqdn[county_idx], this_k)            
        elif 'counties' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.dropout_addr_cnt".format(idx_to_fqdn[this_k])
        elif 'asns' in mode:
            key = "projects.zeusping.test1.routing.asn.{0}.dropout_addr_cnt".format(this_k)

        e_key = key.encode('utf-8')
        idx = kp.get_key(e_key)
        if idx is None:
            idx = kp.add_key(e_key)
        kp.set(idx, n_d)

        if 'county-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.responsive_addr_cnt".format(idx_to_fqdn[county_idx], this_k)
        elif 'counties' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.responsive_addr_cnt".format(idx_to_fqdn[this_k])
        elif 'asns' in mode:
            key = "projects.zeusping.test1.routing.asn.{0}.responsive_addr_cnt".format(this_k)

        e_key = key.encode('utf-8')    
        idx = kp.get_key(e_key)
        if idx is None:
            idx = kp.add_key(e_key)
        kp.set(idx, n_r)

        if 'county-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.antidropout_addr_cnt".format(idx_to_fqdn[county_idx], this_k)
        elif 'counties' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.antidropout_addr_cnt".format(idx_to_fqdn[this_k])
        elif 'asns' in mode:
            key = "projects.zeusping.test1.routing.asn.{0}.antidropout_addr_cnt".format(this_k)

        e_key = key.encode('utf-8')
        idx = kp.get_key(e_key)
        if idx is None:
            idx = kp.add_key(e_key)
        kp.set(idx, n_a)

        if 'numpinged' in mode:
            if 'county-asn' in mode:
                key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.pinged_addr_cnt".format(idx_to_fqdn[county_idx], this_k)
                val = county_asn_to_numpinged[county_idx][this_k]
            elif 'counties' in mode:
                key = "projects.zeusping.test1.geo.netacuity.{0}.pinged_addr_cnt".format(idx_to_fqdn[this_k])
                val = county_to_numpinged[this_k]
            elif 'asns' in mode:
                key = "projects.zeusping.test1.routing.asn.{0}.pinged_addr_cnt".format(this_k)
                val = asn_to_numpinged[this_k]

            e_key = key.encode('utf-8')
            idx = kp.get_key(e_key)
            if idx is None:
                idx = kp.add_key(e_key)
            kp.set(idx, val)




inp_dir = sys.argv[1]
mode = sys.argv[2]

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

counties = idx_to_county.keys()

all_tstamps = set()

# NOTE: We only need to load pyipmeta, ip_to_as when we want to add the timeseries curves for numpinged within the aggregations.
# NOTE: Need to refactor the following code to handle multiple states
if 'numpinged' in mode:

    # Load pyipmeta in order to perform county lookups per address
    ipm = pyipmeta.IpMeta(provider="netacq-edge",
                          provider_config="-b /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz")

    # Load ip to AS
    ip_to_as = {}
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

    county_to_numpinged = defaultdict(int)
    asn_to_numpinged = defaultdict(int)
    county_asn_to_numpinged = {}
    numpinged_fp = open('randsorted_colorado_4M') # TODO: Avoid hardcoding like this!
    
    for line in numpinged_fp:
        ip = line[:-1]
        asn = ip_to_as[ip]
        asn_to_numpinged[asn] += 1
        
        # Find county id
        county_id = '-1'
        res = ipm.lookup(ip)
        if len(res) != 0:
            if 'polygon_ids' in res[0]:
                county_id = str(res[0]['polygon_ids'][0])

        county_to_numpinged[county_id] += 1

        if county_id not in county_asn_to_numpinged:
            county_asn_to_numpinged[county_id] = defaultdict(int)
        county_asn_to_numpinged[county_id][asn] += 1

    del ip_to_as
    ip_to_as = None # Ensure gc
    

if 'counties' in mode:
    county_to_vals = {}
    populate_idx_to_val(county_to_vals, counties)

# TODO: Change the following to include all ASes
# asns = ['7922', '209', '20001']
asns = ['7922', '209', '13977', '22773', '701', '7155']

if 'asns' in mode:
    asn_to_vals = {}
    populate_idx_to_val(asn_to_vals, asns)

if 'county-asn' in mode:
    county_asn_to_vals = {}
    for county_idx in counties:
        if county_idx not in county_asn_to_vals:
            county_asn_to_vals[county_idx] = {}

        # TODO: Call populate_idx_to_val on a county-ASN only if that ASN is pinged in the county perhaps?
        populate_idx_to_val(county_asn_to_vals[county_idx], asns, county_asn=True, county_idx=county_idx)


for tstamp in sorted(all_tstamps):
    if 'counties' in mode:
        set_keys_for_this_tstamp(county_to_vals, counties, tstamp, mode, county_idx=None)
        
    if 'asns' in mode:
        set_keys_for_this_tstamp(asn_to_vals, asns, tstamp, mode, county_idx=None)

    if 'county-asn' in mode:
        for county_idx in county_asn_to_vals:
            set_keys_for_this_tstamp(county_asn_to_vals[county_idx], asns, tstamp, mode, county_idx=county_idx)

    kp.flush(tstamp)    
            
