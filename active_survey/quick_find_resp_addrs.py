#!/usr/bin/env python

# NOTE: Parts of this script are based off find_timeseries_pts.py

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime

mode = sys.argv[1] # Mode can be 'isi' for ISI Hitlist, 'zmap' for Zmap scan, 'zeus' for ZeusPing

# Load pyipmeta in order to perform county lookups per address
# provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
# ipm = pyipmeta.IpMeta(provider="netacq-edge",
#                       provider_config=provider_config_str)


def get_zeus_addrs(addr_filename, annotated_op_fname):

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
            resp_addrs.add(addr) # This address was responsive in the previous round and has the potential to fail
        else:
            unresp_addrs.add(addr)

        if addr in ip_to_usstate:
            usstate = ip_to_usstate[addr]
        else:
            usstate = '-1'

        # Find ASN
        if addr in ip_to_as:
            asn = ip_to_as[addr]
        else:
            asn = '-1'

        annotated_op_fp.write("{0} {1} {2}\n".format(line[:-1], usstate, asn) )            

    return resp_addrs, unresp_addrs


def get_isi_addrs(all_addrs, ip_to_as):

    resp_addrs = set()
    line_ct = 0

    NUM_HISTS = 2 # How many historical censuses we will consider to determine if an address was respsonsive
    
    for line in sys.stdin:

        line_ct += 1

        if line_ct == 1:
            continue

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} isi_hitlist lines read at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )
        
        # if line_ct > 1000:
        #     sys.exit(1)
        
        parts = line.strip().split()

        ip = parts[1].strip()

        hist = parts[2].strip()
        hist = hist[-1] # NOTE: Hack to make this faster temporarily

        hex_int = int(hist, 16)

        resp = 0

        for i in range(NUM_HISTS):
            if hex_int & 1 == 1:
                resp = 1

            hex_int = hex_int >> 1

        # print ip, hist, hex_int, resp

        if resp == 1:
            resp_addrs.add(ip)


    unresp_addrs = all_addrs - resp_addrs

    return resp_addrs, unresp_addrs
        

def update_region_asn_to_status(region_asn_to_status, addrs, mode):

    for addr in addrs:
        # Find region

        if mode == 'zeus':
            if addr in ip_to_usstate:
                usstate = ip_to_usstate[addr]
            else:
                usstate = '-1'

        elif mode == 'isi':
            # TODO: Hack to just ignore usstate for now
            usstate = 'NA'

        # Find ASN
        if addr in ip_to_as:
            asn = ip_to_as[addr]
        else:
            asn = '-1'

        if usstate not in region_asn_to_status:
            region_asn_to_status[usstate] = {}

        if asn not in region_asn_to_status[usstate]:
            region_asn_to_status[usstate][asn] = 0

        region_asn_to_status[usstate][asn] += 1

        
# NOTE: When running this for ISI's hitlist and/or Zmap scans, we'll need some way to get the ASN of each address

if mode == 'zeus':

    addr_filename = sys.argv[2]
    netacq_date = sys.argv[3]

    # NOTE TODO: I am replacing the hacky approach below with a slightly less hacky approach but still one that needs to change eventually. Hardcoding the 20200805 pfx2as suffix is particularly bothersome. Hardcoding the states is also bad.
    # reqd_states = ['LA', 'MS', 'AL', 'AR', 'NH', 'CT', 'MA'] # TODO: Don't hardcode the states
    reqd_states = ['TX', 'MD', 'DE'] # TODO: Don't hardcode the states
    ip_to_as = {}
    ip_to_usstate = {}    
    # ip_to_as_file = sys.argv[4] # Each line of this file is a path to an AS file for a U.S. state    
    # ip_to_as_fp = open(ip_to_as_file)
    # for line in ip_to_as_fp:

    #     parts = line.strip().split()
    #     usstate = parts[0]
    #     usstate_ip_to_as_file = parts[1]

    for usstate in reqd_states:
        usstate_ip_to_as_file = '/scratch/zeusping/probelists/us_addrs/{0}_addrs/all_{0}_addresses_20200805.pfx2as.gz'.format(usstate) # TODO: Don't hardcode the pfx2as date
        ip_to_usstate_as_fp = wandio.open(usstate_ip_to_as_file)
        sys.stderr.write("Opening ip_to_usstate_as_fp for {0} at {1}\n".format(usstate, str(datetime.datetime.now() ) ) )
        line_ct = 0
        # for line in sys.stdin:
        for line in ip_to_usstate_as_fp:

            line_ct += 1

            if line_ct%1000000 == 0:
                sys.stderr.write("{0} ip_to_as lines for {1} read at {2}\n".format(line_ct, usstate, str(datetime.datetime.now() ) ) )

            parts = line.strip().split('|')

            if (len(parts) != 2):
                continue

            addr = parts[0].strip()
            asn = parts[1].strip()

            ip_to_as[addr] = asn
            ip_to_usstate[addr] = usstate

        sys.stderr.write("Done reading ip_to_as for {0} at {1}\n".format(usstate, str(datetime.datetime.now() ) ) )

    # Now that ip_to_asn and ip_to_usstate are loaded, let's walk through the address filename and identify which addresses responded and which addresses did not.
    annotated_op_fname = "{0}_annotated".format(addr_filename)
    resp_addrs, unresp_addrs = get_zeus_addrs(addr_filename, annotated_op_fname)

    
elif mode == 'isi':
    
    ip_to_as_file = sys.argv[2]
    # ip_to_usstate = {}
    ip_to_as = {}

    all_addrs = set() # Set that contains all addresses in Netacuity + pfx2as for a given country that we're exploring

    ip_to_as_fp = wandio.open(ip_to_as_file)
    line_ct = 0
    for line in ip_to_as_fp:

        line_ct += 1

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} ip_to_as lines read at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )

        parts = line.strip().split('|')

        if (len(parts) != 2):
            continue

        addr = parts[0].strip()
        asn = parts[1].strip()

        ip_to_as[addr] = asn

        all_addrs.add(addr)

    sys.stderr.write("Done reading ip_to_as at {0}\n".format(str(datetime.datetime.now() ) ) )

    resp_addrs, unresp_addrs = get_isi_addrs(all_addrs, ip_to_as)
    
    
region_asn_to_status = {"resp" : {}, "unresp" : {}}
# Maybe this can eventually even have county-level data. For now, I will begin with state-level data.
update_region_asn_to_status(region_asn_to_status["resp"], resp_addrs, mode)
update_region_asn_to_status(region_asn_to_status["unresp"], unresp_addrs, mode)

if mode == 'zeus':
    op_fname = "{0}_regionasn_to_status".format(addr_filename)
elif mode == 'isi':
    op_fname = "temp"
op_fp = open(op_fname, "w")

for usstate in region_asn_to_status["resp"]:
    for asn in region_asn_to_status["resp"][usstate]:

        tot_resp = region_asn_to_status["resp"][usstate][asn]
        tot_unresp = 0
        
        if usstate in region_asn_to_status["unresp"]:
            if asn in region_asn_to_status["unresp"][usstate]:
                tot_unresp = region_asn_to_status["unresp"][usstate][asn]

        resp_pct = (tot_resp/float(tot_resp + tot_unresp)) * 100.0

        op_fp.write("{0} {1} {2} {3} {4:.4f}\n".format(usstate, asn, tot_resp, tot_unresp, resp_pct) )
