#!/usr/bin/env python

# NOTE: Parts of this script are based off find_timeseries_pts.py
# 8GB for 75M resp addrs

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime
import ipaddress
import radix
import struct
import socket

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


# NOTE: We should *not* assume that an ip_to_as_file always contains IP2AS mappings for a specific region. For example, if we are loading ip_to_as for ISI responsive addresses, the addresses will geolocate to several different places. However, if we are loading ip_to_as for addresses in CA, then region_name is CA. That is why ip_to_region is an optional argument to this function.
def populate_ip_to_as(ip_to_as_file, ip_to_as, reqd_asns=None, reqd_addrs=None, region_name=None, ip_to_region=None):
    
    ip_to_as_fp = wandio.open(ip_to_as_file)
    line_ct = 0

    sys.stderr.write("Opening ip_to_as for {0} at {1}\n".format(ip_to_as_file, str(datetime.datetime.now() ) ) )
    
    for line in ip_to_as_fp:

        line_ct += 1

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} ip_to_as lines for {1} read at {2}\n".format(line_ct, ip_to_as_file, str(datetime.datetime.now() ) ) )

        parts = line.strip().split('|')

        if (len(parts) != 2):
            continue

        if reqd_asns is None:
            addr = parts[0].strip()

            if reqd_addrs is None:
                asn = parts[1].strip()
                ip_to_as[addr] = asn
                if region_name is not None:
                    ip_to_region[addr] = region_name
            else:
                if addr in reqd_addrs:
                    asn = parts[1].strip()
                    ip_to_as[addr] = asn
                    if region_name is not None:
                        ip_to_region[addr] = region_name
        else:
            asn = parts[1].strip()

            if asn in reqd_asns:
                addr = parts[0].strip()

                if reqd_addrs is None:
                    ip_to_as[addr] = asn
                    if region_name is not None:
                        ip_to_region[addr] = region_name
                else:
                    if addr in reqd_addrs:
                        ip_to_as[addr] = asn
                        if region_name is not None:
                            ip_to_region[addr] = region_name
                        
    sys.stderr.write("Done reading ip_to_as for {0} at {1}\n".format(ip_to_as_file, str(datetime.datetime.now() ) ) )                

    
def obtain_reqdips_from_ip2as_file(ip_to_as_file, aggr_addrs):

    reqd_addrs = set()
    
    ip_to_as_fp = wandio.open(ip_to_as_file)
    line_ct = 0

    sys.stderr.write("Opening ip_to_as for {0} at {1}\n".format(ip_to_as_file, str(datetime.datetime.now() ) ) )
    
    for line in ip_to_as_fp:

        line_ct += 1

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} ip_to_as lines for {1} read at {2}\n".format(line_ct, ip_to_as_file, str(datetime.datetime.now() ) ) )

        parts = line.strip().split('|')

        if (len(parts) != 2):
            continue

        addr = parts[0].strip()

        if addr in aggr_addrs:
            reqd_addrs.add(addr)

    return reqd_addrs
        

def populate_usstate_to_reqd_asns(usstate_to_reqd_asns_fname, usstate_to_reqd_asns):
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


def get_zeus_addrs(addr_filename, annotated_op_fname, ip_to_as, ip_to_usstate):

    annotated_op_fp = open(annotated_op_fname, "w")
    
    resp_addrs = set()
    unresp_addrs = set()
    fp = open(addr_filename, 'r')
    for line in fp:
        parts = line.strip().split()
        addr = parts[0].strip()

        sent_pkts = int(parts[1])
        successful_resps = int(parts[2])
        host_unreach = int(parts[3])
        icmp_err = int(parts[4])
        losses = int(parts[5])

        if successful_resps > 0:
            resp_addrs.add(addr)
        else:
            unresp_addrs.add(addr)

        # if addr in ip_to_usstate:
        #     usstate = ip_to_usstate[addr]
        # else:
        #     usstate = '-1'
        usstate = ip_to_usstate.get(addr, '-1')

        # Find ASN
        # if addr in ip_to_as:
        #     asn = ip_to_as[addr]
        # else:
        #     asn = '-1'
        asn = ip_to_as.get(addr, '-1')

        annotated_op_fp.write("{0} {1} {2}\n".format(line[:-1], usstate, asn) )            

    return resp_addrs, unresp_addrs
        

def update_region_asn_to_status(region_asn_to_status, addrs, ip_to_as=None, ip_to_region=None, rtree=None, must_find_region=False, ipm=None, idx_to_region_name=None, is_US=False, temp_suf=None, is_ipid=False):

    for addr in addrs:
        # Find region

        if is_ipid == True:
            # If we had stored the addrs as 32-byte integers for more
            # efficient memory usage, obtain the ip_str from the
            # addr. We need the ip_str for finding the ip's region and
            # the ip's AS.
            addr_str = socket.inet_ntoa(struct.pack('!L', addr))
            addr = addr_str

        region = '-1'
        if ip_to_region is not None:
            if addr in ip_to_region:
                region = ip_to_region[addr]
        elif must_find_region is True:
            res = ipm.lookup(addr)

            ctry_code = res[0]['country_code']

            if is_US is True:
                if ctry_code != 'US':
                    continue

                loc_id = str(res[0]['polygon_ids'][1]) # This is for region info

                if (loc_id) in idx_to_region_name:
                    loc_name = idx_to_region_name[(loc_id)]
                    region = loc_name

            else:
                region = ctry_code


        # Find ASN
        if ip_to_as is not None:
            asn = ip_to_as.get(addr, 'UNK')
        elif rtree is not None:

            # try:
            #     ip = ipaddress.ip_address(unicode(addr))
            # except:
            #     sys.stderr.write("Bad ip: {0}\n".format(addr) )
            #     continue
            
            rnode = rtree.search_best(addr)
            if rnode is None:
                asn = 'UNK'
            else:
                asn = rnode.data["origin"]
                
        if region not in region_asn_to_status:
            region_asn_to_status[region] = {}

        if asn not in region_asn_to_status[region]:
            region_asn_to_status[region][asn] = 0
            # region_asn_to_status[region][asn] = set()

        region_asn_to_status[region][asn] += 1
        # region_asn_to_status[region][asn].add(addr)

        
def write_region_asn_to_status(region_asn_to_status, op_fname):
    op_fp = open(op_fname, "w")
    # temp_test_fp = open("temp_testing_allasns_verbose", "w")
    
    for region in region_asn_to_status["resp"]:
        for asn in region_asn_to_status["resp"][region]:

            # for addr in region_asn_to_status["resp"][region][asn]:
            #     temp_test_fp.write("{0} {1} {2} R\n".format(region, asn, addr) )

            tot_resp = region_asn_to_status["resp"][region][asn]
            # tot_resp = len(region_asn_to_status["resp"][region][asn])
            
            tot_unresp = 0
            if region in region_asn_to_status["unresp"]:
                if asn in region_asn_to_status["unresp"][region]:

                    # for addr in region_asn_to_status["unresp"][region][asn]:
                    #     temp_test_fp.write("{0} {1} {2} U\n".format(region, asn, addr) )
                    
                    tot_unresp = region_asn_to_status["unresp"][region][asn]
                    # tot_unresp = len(region_asn_to_status["unresp"][region][asn])

            resp_pct = (tot_resp/float(tot_resp + tot_unresp)) * 100.0

            op_fp.write("{0}|{1}|{2}|{3}|{4}|{5:.4f}\n".format(region, asn, tot_resp, tot_unresp, (tot_resp + tot_unresp), resp_pct) )

            
def find_zeus_resp_unresp(pinged_ip_to_as, pinged_ip_to_usstate, fn_mode='usstates'):
    
    # Find the region and AS of each pinged address. Since we had previously populated this information, we just need to load the correct files
    # Among pinged addresses, find who responded and who did not (addr_filename)
    # Find % responsive per AS

    addr_filename = sys.argv[2]
    netacq_date = sys.argv[3]

    # Load pinged_ip_to_as and pinged_ip_to_usstate using previously crunched files
    if fn_mode == 'usstates':
        pfx2as_date = sys.argv[4]
        usstate_to_reqd_asns_fname = sys.argv[5]
        usstate_to_reqd_asns = {}
        populate_usstate_to_reqd_asns(usstate_to_reqd_asns_fname, usstate_to_reqd_asns)

        for usstate in usstate_to_reqd_asns:
            usstate_ip_to_as_file = '/scratch/zeusping/probelists/us_addrs/{0}_addrs/all_{0}_addresses_{1}.pfx2as.gz'.format(usstate, pfx2as_date)

            populate_ip_to_as(usstate_ip_to_as_file, pinged_ip_to_as, reqd_asns=usstate_to_reqd_asns[usstate], region_name=usstate, ip_to_region=pinged_ip_to_usstate)

    elif fn_mode == 'givenip2as':
        ip_to_as_file = sys.argv[4]
        populate_ip_to_as(ip_to_as_file, pinged_ip_to_as)
        
    # Now that pinged_ip_to_asn and pinged_ip_to_usstate are loaded, let's walk through the address filename and identify which addresses responded and which addresses did not.
    annotated_op_fname = "{0}_annotated".format(addr_filename)
    resp_addrs, unresp_addrs = get_zeus_addrs(addr_filename, annotated_op_fname, pinged_ip_to_as, pinged_ip_to_usstate)

    return resp_addrs, unresp_addrs, addr_filename

    
def find_isinetacq_resp_unresp(netacq_addrs_ip_to_as):
    # Find the AS of each potential address that could/should have been pinged in an aggregate  (country-AS, say), according to Netacuity + pfx2AS
    # Of these potential addresses, find the subset that responded. 
    # Find % responsive per AS
    
    netacq_addrs_ip_to_as_file = sys.argv[2]

    populate_ip_to_as(netacq_addrs_ip_to_as_file, netacq_addrs_ip_to_as)

    # Let's get a set that contains all addresses in Netacuity + pfx2as for a given aggregate that we're exploring
    # netacq_addrs = netacq_addrs_ip_to_as.keys() # NOTE: keys() does not get the values as a set but as a list. But I want to perform set operations. So let's use viewkeys() instead which yields a set-like object.
    netacq_addrs = netacq_addrs_ip_to_as.viewkeys() 
    
    resp_ip_to_as_file = sys.argv[3]
    resp_ip_to_as = {}
    # resp_ip_to_region = {}
    # Let's populate_ip_to_as *only* for the ASes netacq_addrs belong to
    # NOTE: The scheme to populate_ip_to_as for *ASes where netacq_addrs belong* was not 100% accurate. Turned out that addresses belonging to an AS can sometimes geolocate to different countries (some addresses in AS58224 did not geolocate to Iran, geolocating instead to UAE and a handful of addresses in AS43754 geolocated to Brazil of all places). Thus, a more accurate version is to only identify responsive_addresses that are a strict subset of netacq_addrs.
    # reqd_asns = set(netacq_addrs_ip_to_as.viewvalues() )
    # populate_ip_to_as(resp_ip_to_as_file, resp_ip_to_as, reqd_asns=reqd_asns)
    # populate_ip_to_as(resp_ip_to_as_file, resp_ip_to_as, reqd_addrs=netacq_addrs)
    # resp_addrs = resp_ip_to_as.viewkeys()
    resp_addrs = obtain_reqdips_from_ip2as_file(resp_ip_to_as_file, netacq_addrs)

    unresp_addrs = netacq_addrs - resp_addrs

    # print len(netacq_addrs)
    # print len(resp_addrs)
    # print len(unresp_addrs)

    # ip_to_as = netacq_addrs_ip_to_as # update_region_asn_to_status() requires a variable called ip_to_as

    return resp_addrs, unresp_addrs


def update_region_asn_to_status_isipinged(region_asn_to_status, addrs_ip_to_as_file, idx_to_region_name, rtree, is_US=False, ipm=None, temp_suf=None):

    # temp_fp = open('/scratch/zeusping/data/quick_census/isi/temp_annotated_{0}'.format(temp_suf), 'w')
    
    # addrs = set()
    
    ip_to_as_fp = wandio.open(addrs_ip_to_as_file)
    line_ct = 0

    sys.stderr.write("Opening ip_to_as for {0} at {1}\n".format(addrs_ip_to_as_file, str(datetime.datetime.now() ) ) )
    
    for line in ip_to_as_fp:

        line_ct += 1

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} ip_to_as lines for {1} read at {2}\n".format(line_ct, addrs_ip_to_as_file, str(datetime.datetime.now() ) ) )

        parts = line.strip().split('|')

        if (len(parts) != 2):
            continue

        addr = parts[0].strip()

        region = '-1'
        
        res = ipm.lookup(addr)

        ctry_code = res[0]['country_code']
        
        if is_US is True:
            if ctry_code == 'US':
                loc_id = str(res[0]['polygon_ids'][1]) # This is for region info

                if (loc_id) in idx_to_region_name:
                    loc_name = idx_to_region_name[(loc_id)]
                    region = loc_name
                    
                rnode = rtree.search_best(addr)
                if rnode is None:
                    asn = 'UNK'
                else:
                    asn = rnode.data["origin"]

                if region not in region_asn_to_status:
                    region_asn_to_status[region] = {}

                if asn not in region_asn_to_status[region]:
                    region_asn_to_status[region][asn] = 0
                    # region_asn_to_status[region][asn] = set()

                region_asn_to_status[region][asn] += 1
                # region_asn_to_status[region][asn].add(addr)
                
                # temp_fp.write("{0}|{1}|{2}\n".format(addr, region, asn) )
    
        else:
            if ctry_code != 'US':
                region = ctry_code
                
                rnode = rtree.search_best(addr)
                if rnode is None:
                    asn = 'UNK'
                else:
                    asn = rnode.data["origin"]

                if region not in region_asn_to_status:
                    region_asn_to_status[region] = {}

                if asn not in region_asn_to_status[region]:
                    region_asn_to_status[region][asn] = 0
                    # region_asn_to_status[region][asn] = set()

                region_asn_to_status[region][asn] += 1
                # region_asn_to_status[region][asn].add(addr)

                # temp_fp.write("{0}|{1}|{2}\n".format(addr, region, asn) )

    
def find_isipinged_resp_unresp_sampledases(isipinged_addrs_ip_to_as, resp_addrs_ip_to_as_file, unresp_addrs_ip_to_as_file, reqd_asns):
    
    populate_ip_to_as(resp_addrs_ip_to_as_file, isipinged_addrs_ip_to_as, reqd_asns=reqd_asns)
    # resp_addrs = isipinged_addrs_ip_to_as.viewkeys() # viewkeys() is a pointer to the dict's keys; if we update the dict later, it will correspondingly update the keys as well. We need a static snapshot of the keys in isipinged_addrs_ip_to_as at this moment.
    resp_addrs = set()
    for addr in isipinged_addrs_ip_to_as:
        resp_addrs.add(addr)

    populate_ip_to_as(unresp_addrs_ip_to_as_file, isipinged_addrs_ip_to_as, reqd_asns=reqd_asns)
    all_addrs = isipinged_addrs_ip_to_as.viewkeys()

    unresp_addrs = all_addrs - resp_addrs

    return resp_addrs, unresp_addrs

    
mode = sys.argv[1] # Mode can be 'isi' for ISI Hitlist, 'zmap' for Zmap scan, 'zeus' for ZeusPing            
region_asn_to_status = {"resp" : {}, "unresp" : {}}

# NOTE: I've assumed that we've already found the AS of each address using ipmeta-lookup
if mode == 'zeus' or mode == 'zeus-givenip2as':
    pinged_ip_to_as = {}
    pinged_ip_to_usstate = {}

    # TODO: Move sys.argv from inside find_zeus_resp_unresp to here.
    if mode == 'zeus':
        resp_addrs, unresp_addrs, addr_filename = find_zeus_resp_unresp(pinged_ip_to_as, pinged_ip_to_usstate, fn_mode='usstates')
    elif mode == 'zeus-givenip2as':
        # This mode is when we provide the ip2as file for all ips in addr_filename in a single file specified as a command-line argument
        resp_addrs, unresp_addrs, addr_filename = find_zeus_resp_unresp(pinged_ip_to_as, pinged_ip_to_usstate, fn_mode='givenip2as')
    update_region_asn_to_status(region_asn_to_status["resp"], resp_addrs, pinged_ip_to_as, pinged_ip_to_usstate)
    update_region_asn_to_status(region_asn_to_status["unresp"], unresp_addrs, pinged_ip_to_as, pinged_ip_to_usstate)
    op_fname = "{0}_regionasn_to_status".format(addr_filename)
    
# This is the "isi-netacq" approach, where we determine the set of all addresses in an aggregate using Netacuity + pfx2as, instead of using the addresses that had been pinged by ISI    
elif mode == 'isi-netacq':
    netacq_addrs_ip_to_as = {}
    # TODO: Move sys.argv from inside find_isinetacq_resp_unresp to here and test.
    resp_addrs, unresp_addrs = find_isinetacq_resp_unresp(netacq_addrs_ip_to_as)
    update_region_asn_to_status(region_asn_to_status["resp"], resp_addrs, netacq_addrs_ip_to_as)
    update_region_asn_to_status(region_asn_to_status["unresp"], unresp_addrs, netacq_addrs_ip_to_as)
    aggr = sys.argv[4]
    op_fname = "./data/{0}_isinetacqaddrs".format(aggr)

elif mode == 'isi-pinged':

    resp_addrs_ip_to_as_file = sys.argv[2]
    unresp_addrs_ip_to_as_file = sys.argv[3]    
    aggr = sys.argv[4] # TODO: Think about whether we should statically define aggr depending upon the is_US flag.
    op_fname = "./data/{0}_isipingedaddrs".format(aggr)
    pfx2AS_fn = sys.argv[5]
    netacq_date = sys.argv[6]
    desired_loc = sys.argv[7]
    
    # Technique to look into specific ASes that we specify. This run completed in ~45 minutes.
    # reqd_asns = {'7018', '5650', '20001', '7922', '701', '22773', '20115', '11272', '209', '33363', '7155', '23089', '6128', '46690'} # Testing
    # reqd_asns = {'7018', '5650', '20001', '7922', '701', '22773', '20115', '209', '7155'} # Testing
    # isipinged_addrs_ip_to_as = {}
    # resp_addrs, unresp_addrs = find_isipinged_resp_unresp_sampledases(isipinged_addrs_ip_to_as, resp_addrs_ip_to_as_file, unresp_addrs_ip_to_as_file, reqd_asns)

    # update_region_asn_to_status(region_asn_to_status["resp"], resp_addrs, must_find_region=True, ipm = ipm, idx_to_region_name=idx_to_region_name)
    # update_region_asn_to_status(region_asn_to_status["unresp"], unresp_addrs, must_find_region=True, ipm = ipm, idx_to_region_name=idx_to_region_name)
    rtree = radix.Radix()
    rnode = zeusping_helpers.load_radix_tree(pfx2AS_fn, rtree)

    # Load pyipmeta in order to perform geo lookups per address
    provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
    ipm = pyipmeta.IpMeta(provider="netacq-edge",
                          provider_config=provider_config_str)

    regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
    idx_to_region_name = {}
    idx_to_region_fqdn = {}
    idx_to_region_code = {}
    zeusping_helpers.load_idx_to_dicts(regions_fname, idx_to_region_fqdn, idx_to_region_name, idx_to_region_code, py_ver=2)

    if desired_loc == "US":
        is_US = True
    else:
        is_US = False

    update_region_asn_to_status_isipinged(region_asn_to_status["resp"], resp_addrs_ip_to_as_file, idx_to_region_name, rtree, is_US=True, ipm=ipm, temp_suf="resp")
    update_region_asn_to_status_isipinged(region_asn_to_status["unresp"], unresp_addrs_ip_to_as_file, idx_to_region_name, rtree, is_US=True, ipm=ipm, temp_suf="unresp")

    
write_region_asn_to_status(region_asn_to_status, op_fname)

