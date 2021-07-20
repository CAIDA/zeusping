#!/usr/bin/env python3

#  This software is Copyright (c) 2020 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes,
#  without fee, and without a written agreement is hereby granted, provided that
#  the above copyright notice, this paragraph and the following three paragraphs
#  appear in all copies. Permission to make use of this software for other than
#  academic research and education purposes may be obtained by contacting:
#
#  Office of Innovation and Commercialization
#  9500 Gilman Drive, Mail Code 0910
#  University of California
#  La Jolla, CA 92093-0910
#  (858) 534-5815
#  invent@ucsd.edu
#
#  This software program and documentation are copyrighted by The Regents of the
#  University of California. The software program and documentation are supplied
#  "as is", without any accompanying services from The Regents. The Regents does
#  not warrant that the operation of the program will be uninterrupted or
#  error-free. The end-user understands that the program was developed for research
#  purposes and is advised not to rely exclusively on the program for any reason.
#
#  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
#  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
#  PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
#  THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
#  IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
#  MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

# TODO: Handle 'numpinged' to find addresses in IR

import sys
import _pytimeseries
import pyipmeta
# import wandio
from collections import defaultdict
import datetime
import shlex
import subprocess

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


def populate_idx_to_val(this_d, list_of_keys, region_asn=False, region_idx='None'):

    for this_k in list_of_keys:

        if region_asn == True:
            # inp_fname = "{0}/resp_dropout_per_round_{1}_AS{2}.gz".format(inp_dir, region_idx, this_k)
            inp_fname = "{0}/resp_dropout_per_round_{1}_AS{2}".format(inp_dir, region_idx, this_k)

        elif 'country' in mode:
            inp_fname = "{0}/resp_dropout_per_round_{1}".format(inp_dir, this_k)
            
        elif 'asns' in mode:
            # inp_fname = "{0}/resp_dropout_per_round_AS{1}.gz".format(inp_dir, this_k)
            inp_fname = "{0}/resp_dropout_per_round_AS{1}".format(inp_dir, this_k)            
        else:
            # inp_fname = "{0}/resp_dropout_per_round_{1}.gz".format(inp_dir, this_k)
            inp_fname = "{0}/resp_dropout_per_round_{1}".format(inp_dir, this_k)

            
        try:
            # sys.stdout.write("{0} is the inp_fname\n".format(inp_fname) )
            # inp_fp = wandio.open(inp_fname, "r")
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


def set_keys_for_this_tstamp(this_d, list_of_keys, tstamp, mode, region_idx=None):
    for this_k in this_d:

        # We have some missing data at 1566523200. So some keys do not have a corresponding entry
        # IMP: Turns out that the keypackage automatically sets keys to 0 if we do not specify them. So even for county_idx 2130, 2011, and 1939, the values are set to 0 for tstamp 1566523200. (Check test_kp.py)
        if tstamp not in this_d[this_k]["n_r"]:
            # sys.stderr.write("No tstamp {0} for key {1}\n".format(tstamp, this_k) )
            continue

        n_d = this_d[this_k]["n_d"][tstamp]
        n_r = this_d[this_k]["n_r"][tstamp]
        n_a = this_d[this_k]["n_a"][tstamp]

        if 'region-asn' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.NA.US.4417.{0}.asn.{1}.dropout_addr_cnt".format(county_idx, this_k)
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.dropout_addr_cnt".format(idx_to_region_fqdn[region_idx], this_k)            
        elif 'regions' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.dropout_addr_cnt".format(idx_to_region_fqdn[this_k])
        elif 'asns' in mode:
            key = "projects.zeusping.test1.routing.asn.{0}.dropout_addr_cnt".format(this_k)
        elif 'countries' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.AS.IR.dropout_addr_cnt"
            key = "projects.zeusping.test1.geo.netacuity.{0}.dropout_addr_cnt".format(ctry_code_to_fqdn[this_k])
        elif 'country-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.dropout_addr_cnt".format(ctry_code_to_fqdn[region_idx], this_k)
        

        # print(key)
        e_key = key.encode('utf-8')
        idx = kp.get_key(e_key)
        if idx is None:
            idx = kp.add_key(e_key)
        kp.set(idx, n_d)

        if 'region-asn' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.previously_responsive_addr_cnt".format(idx_to_region_fqdn[county_idx], this_k)
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.responsive_addr_cnt".format(idx_to_region_fqdn[region_idx], this_k)            
        elif 'regions' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.{0}.previously_responsive_addr_cnt".format(idx_to_region_fqdn[this_k])
            key = "projects.zeusping.test1.geo.netacuity.{0}.responsive_addr_cnt".format(idx_to_region_fqdn[this_k])            
        elif 'asns' in mode:
            # key = "projects.zeusping.test1.routing.asn.{0}.previously_responsive_addr_cnt".format(this_k)
            key = "projects.zeusping.test1.routing.asn.{0}.responsive_addr_cnt".format(this_k)
        elif 'countries' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.AS.IR.responsive_addr_cnt"
            key = "projects.zeusping.test1.geo.netacuity.{0}.responsive_addr_cnt".format(ctry_code_to_fqdn[this_k])
        elif 'country-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.responsive_addr_cnt".format(ctry_code_to_fqdn[region_idx], this_k)
            
        e_key = key.encode('utf-8')            
        idx = kp.get_key(e_key)
        if idx is None:
            idx = kp.add_key(e_key)
        kp.set(idx, n_r)

        if 'region-asn' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.newly_responsive_addr_cnt".format(idx_to_region_fqdn[county_idx], this_k)
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.antidropout_addr_cnt".format(idx_to_region_fqdn[region_idx], this_k)            
        elif 'regions' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.{0}.newly_responsive_addr_cnt".format(idx_to_region_fqdn[this_k])
            key = "projects.zeusping.test1.geo.netacuity.{0}.antidropout_addr_cnt".format(idx_to_region_fqdn[this_k])            
        elif 'asns' in mode:
            # key = "projects.zeusping.test1.routing.asn.{0}.newly_responsive_addr_cnt".format(this_k)
            key = "projects.zeusping.test1.routing.asn.{0}.antidropout_addr_cnt".format(this_k)

        elif 'countries' in mode:
            # key = "projects.zeusping.test1.geo.netacuity.AS.IR.antidropout_addr_cnt"
            key = "projects.zeusping.test1.geo.netacuity.{0}.antidropout_addr_cnt".format(ctry_code_to_fqdn[this_k])
        elif 'country-asn' in mode:
            key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.antidropout_addr_cnt".format(ctry_code_to_fqdn[region_idx], this_k)            

        e_key = key.encode('utf-8')        
        idx = kp.get_key(e_key)
        if idx is None:
            idx = kp.add_key(e_key)
        kp.set(idx, n_a)

        if 'numpinged' in mode:
            if 'region-asn' in mode:
                key = "projects.zeusping.test1.geo.netacuity.{0}.asn.{1}.pinged_addr_cnt".format(idx_to_region_fqdn[region_idx], this_k)
                val = region_asn_to_numpinged[region_idx][this_k]
            elif 'regions' in mode:
                key = "projects.zeusping.test1.geo.netacuity.{0}.pinged_addr_cnt".format(idx_to_region_fqdn[this_k])
                val = region_to_numpinged[this_k]
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
# ctry = sys.argv[3]
loc_to_reqd_asns_fname = sys.argv[3]

# Get idx_to_* dicts for all regions
regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
idx_to_region_name = {}
idx_to_region_fqdn = {}
idx_to_region_code = {}
zeusping_helpers.load_idx_to_dicts(regions_fname, idx_to_region_fqdn, idx_to_region_name, idx_to_region_code)

# for region_idx in idx_to_region_name:
#     sys.stdout.write("{0} {1}\n".format(region_idx, idx_to_region_name[region_idx]) )

# Get idx_to_* dicts for all countries
ctrys_fname = '/data/external/natural-earth/polygons/ne_10m_admin_0.countries.v3.1.0.processed.polygons.csv.gz'
idx_to_ctry_name = {}
idx_to_ctry_fqdn = {}
idx_to_ctry_code = {}
ctry_code_to_fqdn = {}
zeusping_helpers.load_idx_to_dicts(ctrys_fname, idx_to_ctry_fqdn, idx_to_ctry_name, idx_to_ctry_code, ctry_code_to_fqdn=ctry_code_to_fqdn)

loc_to_reqd_asns = {}
loc_to_reqd_asns_fp = open(loc_to_reqd_asns_fname)

for line in loc_to_reqd_asns_fp:
    parts = line.strip().split()
    loc = parts[0].strip()

    if loc not in loc_to_reqd_asns:
        loc_to_reqd_asns[loc] = set()

    asn_list = parts[1].strip()
    asns = asn_list.strip().split('-')

    for asn in asns:
        asns_reqd_splits_parts = asn.strip().split(':')
        loc_to_reqd_asns[loc].add(asns_reqd_splits_parts[0])

asns = set()
for loc in loc_to_reqd_asns:
    for asn in loc_to_reqd_asns[loc]:
        asns.add(asn)

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

regions = idx_to_region_name.keys()

# NOTE: Thus far, we've only processed files from a single country at a time, but I intend for this to change. Thus, when we are processing multiple countries' files in future, we would like to be able to find resp_dropout_per_round_<countrycode> files and process them if available in this directory. To ensure that resp_dropout_per_round_<countrycode> files can be found, I made the necessary changes in find_timeseries_per_ctry_pts.py. 
# ctrys = set(idx_to_ctry_code.values() )
# ctrys.discard('AS') # Remove AS, since we sometimes end up with resp_dropout_per_round_AS files when the Autonomous System is missing. We're likely not going to be pinging American Samoa (the country code that 'AS' corresponds to) in any case...

# Instead of getting all countries in the earth (like our previous iteration), let's just get the countries we probed in this ZeusPing campaign
ctrys = loc_to_reqd_asns.keys()

all_tstamps = set()

# NOTE: We only need to load pyipmeta, ip_to_as when we want to add the timeseries curves for numpinged within the aggregations.
# TODO: Need to refactor the following code to handle Iran
if 'numpinged' in mode:

    # Load pyipmeta in order to perform county lookups per address

    # Load pyipmeta in order to perform county lookups per address
    provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
    ipm = pyipmeta.IpMeta(provider="netacq-edge",
                          provider_config=provider_config_str)
    
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
    

if 'regions' in mode:
    region_to_vals = {}
    populate_idx_to_val(region_to_vals, regions)

if 'countries' in mode:
    country_to_vals = {}
    populate_idx_to_val(country_to_vals, ctrys)


# if ctry == 'IR':
#     asns = [
#         '58224',
#         '197207',
#         '44244',
#         '31549',
#         '12880',
#         '57218',
#         '16322',
#         '56402',
#         '39501',
#         '42337',
#         '50810',
#         '43754',
#         '49100',
#         '24631',
#         '41881',
#         '6736',
#         '25124',
#         '39308',
#         '25184',
#         '51074',
#     ]
# elif ctry == 'GH':
#     asns = [
#         '29614',
#         '35091',
#         '328571',
#         '37074',
#         '328439',
#     ]
    

    
if 'asns' in mode:
    asn_to_vals = {}
    populate_idx_to_val(asn_to_vals, asns)

if 'region-asn' in mode:
    region_asn_to_vals = {}
    for region_idx in regions:
        if region_idx not in region_asn_to_vals:
            region_asn_to_vals[region_idx] = {}

        # TODO: Call populate_idx_to_val on a region-ASN only if that ASN is pinged in the region perhaps?
        populate_idx_to_val(region_asn_to_vals[region_idx], asns, region_asn=True, region_idx=region_idx)

if 'country-asn' in mode:
    ctry_asn_to_vals = {}
    for ctry_code in loc_to_reqd_asns:
        if ctry_code not in ctry_asn_to_vals:
            ctry_asn_to_vals[ctry_code] = {}

        populate_idx_to_val(ctry_asn_to_vals[ctry_code], loc_to_reqd_asns[ctry_code], region_asn=True, region_idx=ctry_code)

    
for tstamp in sorted(all_tstamps):
    if 'regions' in mode:
        set_keys_for_this_tstamp(region_to_vals, regions, tstamp, mode, region_idx=None)
        
    if 'asns' in mode:
        set_keys_for_this_tstamp(asn_to_vals, asns, tstamp, mode, region_idx=None)

    if 'countries' in mode:
        set_keys_for_this_tstamp(country_to_vals, ctrys, tstamp, mode, region_idx=None)        

    if 'region-asn' in mode:
        for region_idx in region_asn_to_vals:
            set_keys_for_this_tstamp(region_asn_to_vals[region_idx], asns, tstamp, mode, region_idx=region_idx)

    if 'country-asn' in mode:
        for ctry_code in ctry_asn_to_vals:
            set_keys_for_this_tstamp(ctry_asn_to_vals[ctry_code], loc_to_reqd_asns[ctry_code], tstamp, mode, region_idx=ctry_code)
            
    kp.flush(tstamp)    
            
