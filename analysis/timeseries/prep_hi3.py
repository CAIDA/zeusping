#!/usr/bin/env python

import sys
import _pytimeseries
import wandio

# Load pyipmeta in order to perform county lookups per address
# ipm = pyipmeta.IpMeta(provider="netacq-edge",
#                       provider_config="-b /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz")

    
def populate_idx_to_val(this_d, list_of_keys, county_asn=False, county_idx='None'):

    for this_k in list_of_keys:

        if county_asn == True:
            inp_fname = "{0}/resp_dropout_per_round_{1}_{2}".format(inp_dir, county_idx, this_k)
        else:
            # TODO: This is a temporary hack because I did not distinguish between AS209 and the county 209.
            if 'counties' in mode and this_k == '209':
                continue
            inp_fname = "{0}/resp_dropout_per_round_{1}".format(inp_dir, this_k)
            
        try:
            inp_fp = open(inp_fname, "r")

        except IOError:
            # sys.stderr.write("{0} could not be opened\n".format(inp_fname) )
            continue

        for line in inp_fp:
            parts = line.strip().split()
            tstamp = int(parts[0].strip() )
            n_r = int(parts[1].strip() )
            n_n = int(parts[2].strip() )
            n_d = int(parts[3].strip() )

            all_tstamps.add(tstamp)

            if this_k not in this_d:
                this_d[this_k] = {"n_d" : {}, "n_r" : {}, "n_n" : {} }
            this_d[this_k]["n_d"][tstamp] = n_d
            this_d[this_k]["n_r"][tstamp] = n_r
            this_d[this_k]["n_n"][tstamp] = n_n            


def set_keys_for_this_tstamp(this_d, list_of_keys, tstamp, mode, county_idx=None):
    for this_k in this_d:

        # We have some missing data at 1566523200. So some keys do not have a corresponding entry
        # IMP: Turns out that the keypackage automatically sets keys to 0 if we do not specify them. So even for county_idx 2130, 2011, and 1939, the values are set to 0 for tstamp 1566523200. (Check test_kp.py)
        if tstamp not in this_d[this_k]["n_r"]:
            # sys.stderr.write("No tstamp {0} for key {1}\n".format(tstamp, this_k) )
            continue

        n_d = this_d[this_k]["n_d"][tstamp]
        n_r = this_d[this_k]["n_r"][tstamp]
        n_n = this_d[this_k]["n_n"][tstamp]

        if 'county-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.dropout_addr_cnt".format(county_idx, this_k)
        elif 'counties' in mode:
            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.dropout_addr_cnt".format(this_k)
        elif 'asns' in mode:
            key = "projects.zeusping.test1.routing.asn.{0}.dropout_addr_cnt".format(this_k)
            
        idx = kp.get_key(key)
        if idx is None:
            idx = kp.add_key(key)
        kp.set(idx, n_d)

        if 'county-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.previously_responsive_addr_cnt".format(county_idx, this_k)
        elif 'counties' in mode:
            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.previously_responsive_addr_cnt".format(this_k)
        elif 'asns' in mode:
            key = "projects.zeusping.test1.routing.asn.{0}.previously_responsive_addr_cnt".format(this_k)

        idx = kp.get_key(key)
        if idx is None:
            idx = kp.add_key(key)
        kp.set(idx, n_r)

        if 'county-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.newly_responsive_addr_cnt".format(county_idx, this_k)
        elif 'counties' in mode:
            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.newly_responsive_addr_cnt".format(this_k)
        elif 'asns' in mode:
            key = "projects.zeusping.test1.routing.asn.{0}.newly_responsive_addr_cnt".format(this_k)
        
        idx = kp.get_key(key)
        if idx is None:
            idx = kp.add_key(key)
        kp.set(idx, n_n)

        
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

idx_to_county = {}
# Populate county_to_id
county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
for line in county_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    county_name = parts[2][1:-1] # Get rid of quotes
    idx_to_county[idx] = county_name

counties = idx_to_county.keys()

all_tstamps = set()

if 'counties' in mode:
    county_to_vals = {}
    populate_idx_to_val(county_to_vals, counties)

asns = ['7922', '209', '20001']

if 'asns' in mode:
    asn_to_vals = {}
    populate_idx_to_val(asn_to_vals, asns)

if 'county-asn' in mode:
    county_asn_to_vals = {}
    for county_idx in counties:
        if county_idx not in county_asn_to_vals:
            county_asn_to_vals[county_idx] = {}
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
            
#     for asn in asn_to_vals:

#         n_d = asn_to_vals[asn]["n_d"][tstamp]
        
#         key = "projects.zeusping.test1.routing.asn.{0}.dropout_addr_cnt".format(asn)
#         idx = kp.get_key(key)
#         if idx is None:
#             idx = kp.add_key(key)
#         kp.set(idx, n_d)

#     kp.flush(tstamp)


# for tstamp in all_tstamps:

#     if 'counties' in mode:
#         set_keys_for_this_tstamp(county_to_vals, counties, tstamp)
    
#     for county_idx in county_asn_to_vals:

#         for asn in county_asn_to_vals[county_idx]:

#             # TODO: Figure out why
#             if tstamp not in county_asn_to_vals[county_idx][asn]["n_d"]:
#                 continue

#             n_d = county_asn_to_vals[county_idx][asn]["n_d"][tstamp]
#             n_r = county_asn_to_vals[county_idx][asn]["n_r"][tstamp]
#             n_n = county_asn_to_vals[county_idx][asn]["n_n"][tstamp]

#             key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.dropout_addr_cnt".format(county_idx, asn)
#             idx = kp.get_key(key)
#             if idx is None:
#                 idx = kp.add_key(key)
#             kp.set(idx, n_d)

#             key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.previously_responsive_addr_cnt".format(county_idx, asn)
#             idx = kp.get_key(key)
#             if idx is None:
#                 idx = kp.add_key(key)
#             kp.set(idx, n_r)

#             key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.newly_responsive_addr_cnt".format(county_idx, asn)
#             idx = kp.get_key(key)
#             if idx is None:
#                 idx = kp.add_key(key)
#             kp.set(idx, n_n)
            
#     kp.flush(tstamp)
    
        

# # add key to key package
# print "Adding Key to Key Package ('a.test.key'):"
# print "Should return 0"
# print kp.add_key("a.test.key")
# print "Adding 'another.test.key', should return 1:"
# print kp.add_key("another.test.key")
# print "Getting index of 'another.test.key', should return 1:"
# print kp.get_key("another.test.key")
# print "Getting index of 'a.test.key', should return 0:"
# print kp.get_key("a.test.key")
# print "Disabling 'a.test.key', should return None:"
# print kp.disable_key(kp.get_key('a.test.key'))
# print "Enabling 'a.test.key', should return None:"
# print kp.enable_key(kp.get_key('a.test.key'))
# print "Getting the current value of 'a.test.key', should return 0:"
# print kp.get(kp.get_key('a.test.key'))
# print "Setting the current value of 'a.test.key' to 12345:"
# print kp.set(kp.get_key('a.test.key'), 12345)
# print "Getting the current value of 'a.test.key', should return 12345:"
# print kp.get(kp.get_key('a.test.key'))
# print "Forcing resolution of all keys, should return None:"
# print kp.resolve()
# print "Getting the number of keys, should return 2:"
# print kp.size
# print "Disabling 'another.test.key' and getting enabled size, should return 1:"
# kp.disable_key(kp.get_key('another.test.key'))
# print kp.enabled_size
# print "Flushing key package, should output 1 line of metrics and then None:"
# print kp.flush(532051200)
# print "Enabling 'another.test.key' and flushing, should output 2 lines of data:"
# kp.enable_key(kp.get_key('another.test.key'))
# kp.flush(532051200)
# print


# print "done!"
