#!/usr/bin/env python

import sys
import _pytimeseries
import wandio

# Load pyipmeta in order to perform county lookups per address
# ipm = pyipmeta.IpMeta(provider="netacq-edge",
#                       provider_config="-b /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/2019-08-09.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz")

idx_to_county = {}
# Populate county_to_id
county_idxs_fp = wandio.open('/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz')
for line in county_idxs_fp:
    parts = line.strip().split(',')
    idx = parts[0].strip()
    county_name = parts[2][1:-1] # Get rid of quotes
    idx_to_county[idx] = county_name

    
inp_dir = sys.argv[1]

ts = _pytimeseries.Timeseries()

# print ts
# print

# print "Asking for ASCII backend by ID:"
# # try getting a backend that exists
# be = ts.get_backend_by_id(1)
# print "Got backend: %d, %s (%s)" % (be.id, be.name, be.enabled)
# print

# # try to get one that does not exist
# print "Asking for non-existent backend by ID (1000):"
# be = ts.get_backend_by_id(1000)
# print "This should be none: %s" % be
# print

# # try to get all available backends
# print "Getting all available backends:"
# all_bes = ts.get_all_backends()
# print all_bes
# print

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



asns = ['7922', '209', '20001']

# asn_to_vals = {}
# all_tstamps = []

# for asn in asns:
#     inp_fp = open("{0}/resp_dropout_per_round_{1}".format(inp_dir, asn), "r")
    
#     for line in inp_fp:
#         parts = line.strip().split()
#         tstamp = int(parts[0].strip() )
#         n_r = int(parts[1].strip() )
#         n_n = int(parts[2].strip() )
#         n_d = int(parts[3].strip() )

#         all_tstamps.append(tstamp)
        
#         if asn not in asn_to_vals:
#             asn_to_vals[asn] = {"n_d" : {}, "n_r" : {}, "n_n" : {} }
#         asn_to_vals[asn]["n_d"][tstamp] = n_d


county_asn_to_vals = {}
all_tstamps = []
for county_idx in idx_to_county:
    for asn in asns:
        try:
            inp_fp = open("{0}/resp_dropout_per_round_{1}_{2}".format(inp_dir, county_idx, asn), "r")
        except IOError:
            continue

        for line in inp_fp:
            parts = line.strip().split()
            tstamp = int(parts[0].strip() )
            n_r = int(parts[1].strip() )
            n_n = int(parts[2].strip() )
            n_d = int(parts[3].strip() )

            all_tstamps.append(tstamp)

            if county_idx not in county_asn_to_vals:
                county_asn_to_vals[county_idx] = {}
            if asn not in county_asn_to_vals[county_idx]:
                county_asn_to_vals[county_idx][asn] = {"n_d" : {}, "n_r" : {}, "n_n" : {} }
            county_asn_to_vals[county_idx][asn]["n_d"][tstamp] = n_d

        
# for tstamp in all_tstamps:

#     for asn in asn_to_vals:

#         n_d = asn_to_vals[asn]["n_d"][tstamp]
        
#         key = "projects.zeusping.test1.routing.asn.{0}.dropout_addr_cnt".format(asn)
#         idx = kp.get_key(key)
#         if idx is None:
#             idx = kp.add_key(key)
#         kp.set(idx, n_d)

#     kp.flush(tstamp)


for tstamp in all_tstamps:
    for county_idx in county_asn_to_vals:
        for asn in county_asn_to_vals[county_idx]:

            # TODO: Figure out why
            if tstamp not in county_asn_to_vals[county_idx][asn]["n_d"]:
                continue
            
            n_d = county_asn_to_vals[county_idx][asn]["n_d"][tstamp]

            key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.dropout_addr_cnt".format(county_idx, asn)
            idx = kp.get_key(key)
            if idx is None:
                idx = kp.add_key(key)
            kp.set(idx, n_d)

    kp.flush(tstamp)
    
        

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
