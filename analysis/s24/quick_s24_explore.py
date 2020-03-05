
# 1. Identify a 10-minute round where an AS had a spike in dropouts
# 2. Identify each /24 of that AS
# 3. Identify each /24's counties and number of responsive, number of dropout addresses, number of anti-dropout addresses
# 4. For /24s with at least one dropout in this round: Also identify each /24's estimates of dropouts in the previous N rounds (d_prev[0, 1, 2, 3...]), dropouts in the next N rounds d_post[0, 1, 2, 3...]), all dropouts from previous N rounds to next N rounds (n_d_prev_to_post, which is a union) and set of addresses that were responsive from previous N rounds to next N rounds (n_r_prev_to_post)

import sys
import pyipmeta
from collections import namedtuple
import wandio
import datetime
import dateutil
from dateutil.parser import parse

inp_fname = sys.argv[1] # Just pass in the input filename
reqd_asn = sys.argv[2]
addr_metadata_fname = sys.argv[3]
op_pref = sys.argv[4]

ip_to_metadata = {}
addr_metadata_fp = wandio.open(addr_metadata_fname)
for line in addr_metadata_fp:
    parts = line.strip().split()
    ip = parts[0].strip()
    asn = parts[4].strip()
    county_id = parts[5].strip()
    
    county_name = ''
    for elem in parts[6:]:
        county_name += '{0} '.format(elem)
    

    ip_to_metadata[ip] = {"asn" : asn, "county_id" : county_id, "county_name" : county_name}


def split_ips(ip):
    parts = ip.strip().split('.')
    # print parts
    oct1 = parts[0].strip()
    oct2 = parts[1].strip()
    oct3 = parts[2].strip()
    oct4 = parts[3].strip()

    return oct1, oct2, oct3, oct4


def find_s24(ipv4_addr):
    oct1, oct2, oct3, oct4 = split_ips(ipv4_addr)
    return "{0}.{1}.{2}.0/24".format(oct1, oct2, oct3)


s24_to_status = {}
# dropout_s24s contains s24s which had at least one dropout in the specified round. We will use dropout_s24s for efficiency. Although we have data for other /24s in this round, we care only about /24s which had at least one dropout in this analysis
dropout_s24s = set()
inp_fp = wandio.open(inp_fname)
for line in inp_fp:
    parts = line.strip().split()

    if len(parts) != 2:
        sys.stderr.write("Weird line length: {0}\n".format(line) )
        sys.exit(1)
    
    addr = parts[0].strip()
    status = parts[1].strip()

    if status == '0':
        status = 'd'
    elif status == '1':
        status = 'r'
    elif status == '2':
        status = 'a'
    

    if addr in ip_to_metadata:
        if ip_to_metadata[addr]["asn"] == reqd_asn:
            s24 = find_s24(addr)
            
            if s24 not in s24_to_status:
                s24_to_status[s24] = {}
            # county_id = ip_to_metadata[addr]["county_id"]
            county_name = ip_to_metadata[addr]["county_name"]            
            
            # if county_id not in s24_to_status[s24]:
            if county_name not in s24_to_status[s24]:
                # s24_to_status[s24][county_id] = {"r" : 0, "d" : 0, "a" : 0}                
                s24_to_status[s24][county_name] = {"r" : 0, "d" : 0, "a" : 0}
                
            # s24_to_status[s24][county_id][status] += 1
            s24_to_status[s24][county_name][status] += 1            

            if status == 'd':
                dropout_s24s.add(s24)



inp_fname_parts = inp_fname.strip().split('_')
round_end_time_epoch = int(inp_fname_parts[-1][:-3])
round_start_time_epoch = round_end_time_epoch - 600

this_h_dt = datetime.datetime.utcfromtimestamp(round_start_time_epoch)
this_h_dt_str = this_h_dt.strftime("%Y_%m_%d_%H_%M")


# for s24 in s24_to_status:
op_s24_fname = './data/{0}_{1}_to_{2}_{3}_s24'.format(this_h_dt_str, round_start_time_epoch, round_end_time_epoch, asn)
op_s24_fp = open(op_s24_fname, 'w')
op_county_s24_fname = './data/{0}_{1}_to_{2}_{3}_county_s24'.format(this_h_dt_str, round_start_time_epoch, round_end_time_epoch, asn)
op_county_s24_fp = open(op_county_s24_fname, 'w')
# op_pref = open(sys.argv[4], 'w')
for s24 in dropout_s24s:
    total_r = 0
    total_d = 0
    total_a = 0
    for county_id in s24_to_status[s24]:
        r = s24_to_status[s24][county_id]["r"]
        d = s24_to_status[s24][county_id]["d"]
        a = s24_to_status[s24][county_id]["a"]
        total_r += r
        total_d += d
        total_a += a
        
        op_county_s24_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(s24, county_id, r, d, a) )

    op_s24_fp.write("{0}|{1}|{2}|{3}\n".format(s24, total_r, total_d, total_a ) )    
