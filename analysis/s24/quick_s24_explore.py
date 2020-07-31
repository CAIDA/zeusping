
# 1. Identify a 10-minute round where an AS had a spike in dropouts
# 2. Identify each /24 of that AS
# 3. Identify each /24's counties and number of responsive, number of dropout addresses, number of anti-dropout addresses
# 4. For /24s with at least one dropout in this round: Also identify each /24's estimates of dropouts in the previous N rounds (d_prev[0, 1, 2, 3...]), dropouts in the next N rounds d_post[0, 1, 2, 3...]), all dropouts from previous N rounds to next N rounds (n_d_prev_to_post, which is a union) and set of addresses that were responsive from previous N rounds to next N rounds (n_r_prev_to_post, which is an intersection)

# Open previous file by subtracting 600 * N from start_time.

# NOTE NOTE: Dropouts and anti-dropouts are unions of addresses. Responsive addresses are intersections of addresses across *all* rounds. Thus, addresses that dropped out, or that had an anti-dropout, will never be responsive. Consider, for example an anti-dropout address in round -N. Since that address hadn't been considered "responsive" at the beginning of round -N (it hadn't been responsive in round -(N+1) and it would have a status of "anti-dropout" in round -N), that address will be absent from at least one round's responsive address set and will therefore not be a part of the union.
# TODO: Print sorted union of responsive addresses from each /24


import sys
# import pyipmeta
from collections import namedtuple
import wandio
import datetime
import dateutil
from dateutil.parser import parse
import shlex
import subprocess

inp_path = sys.argv[1]
d_round_epoch= int(sys.argv[2])
reqd_asn = sys.argv[3]
addr_metadata_fname = sys.argv[4]
num_adjacent_rounds = int(sys.argv[5])
is_compressed = int(sys.argv[6])
# op_pref = sys.argv[4]

ROUND_SECS = 10 * 60 # Number of seconds in a 10-minute round

ip_to_metadata = {}
if is_compressed == 1:
    addr_metadata_fp = wandio.open(addr_metadata_fname)
else:
    addr_metadata_fp = open(addr_metadata_fname)
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


s24_to_status_set = {}
for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):
    s24_to_status_set[roun] = {}

s24_to_status = {}
# dropout_s24s contains s24s which had at least one dropout in the specified round. We will use dropout_s24s for memory efficiency by preventing s24_to_status_set from becoming too large. Although we have data from other /24s in this round, we care only about /24s which had at least one dropout in this analysis
dropout_s24s = set()
if is_compressed == 1:
    inp_fname = "{0}/{1}_to_{2}.gz".format(inp_path, d_round_epoch, d_round_epoch + ROUND_SECS )
    inp_fp = wandio.open(inp_fname)
else:
    inp_fname = "{0}/{1}_to_{2}".format(inp_path, d_round_epoch, d_round_epoch + ROUND_SECS )
    inp_fp = open(inp_fname)
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

            if s24 not in s24_to_status_set[0]:
                s24_to_status_set[0][s24] =  {"d" : set(), "r" : set(), "a": set()}
                
            # county_id = ip_to_metadata[addr]["county_id"]
            county_name = ip_to_metadata[addr]["county_name"]            
            
            # if county_id not in s24_to_status[s24]:
            if county_name not in s24_to_status[s24]:
                # s24_to_status[s24][county_id] = {"r" : 0, "d" : 0, "a" : 0}                
                s24_to_status[s24][county_name] = {"r" : 0, "d" : 0, "a" : 0}
                
            # s24_to_status[s24][county_id][status] += 1
            s24_to_status[s24][county_name][status] += 1

            s24_to_status_set[0][s24][status].add(addr)

            if status == 'd':
                dropout_s24s.add(s24)



for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):

    # We've already finished the 0th round
    if roun == 0:
        continue
    
    temp_round_tstart = d_round_epoch + roun*ROUND_SECS

    if is_compressed == 1:
        temp_inp_fname = "{0}/{1}_to_{2}.gz".format(inp_path, temp_round_tstart, temp_round_tstart + ROUND_SECS)
        temp_inp_fp = wandio.open(temp_inp_fname)
    else:
        temp_inp_fname = "{0}/{1}_to_{2}".format(inp_path, temp_round_tstart, temp_round_tstart + ROUND_SECS)
        temp_inp_fp = open(temp_inp_fname)

    for line in temp_inp_fp:
        parts = line.strip().split()

        if len(parts) != 2:
            sys.stderr.write("Weird line length: {0}\n".format(line) )
            sys.exit(1)

        addr = parts[0].strip()
        s24 = find_s24(addr)
        
        if s24 in dropout_s24s:
            
            status = parts[1].strip()

            if status == '0':
                status = 'd'
            elif status == '1':
                status = 'r'
            elif status == '2':
                status = 'a'

            if s24 not in s24_to_status_set[roun]:
                s24_to_status_set[roun][s24] =  {"d" : set(), "r" : set(), "a": set()}

            s24_to_status_set[roun][s24][status].add(addr)
                
                
# inp_fname_parts = inp_fname.strip().split('_')
# if is_compressed == 1:
#     round_end_time_epoch = int(inp_fname_parts[-1][:-3])
# else:
#     round_end_time_epoch = int(inp_fname_parts[-1])
# round_start_time_epoch = round_end_time_epoch - ROUND_SECS

# this_h_dt = datetime.datetime.utcfromtimestamp(round_start_time_epoch)
# this_h_dt_str = this_h_dt.strftime("%Y_%m_%d_%H_%M")

this_h_dt = datetime.datetime.utcfromtimestamp(d_round_epoch)
this_h_dt_str = this_h_dt.strftime("%Y_%m_%d_%H_%M")

d_round_endtime_epoch = d_round_epoch + ROUND_SECS

op_dir = '{0}_{1}_to_{2}'.format(this_h_dt_str, d_round_epoch, d_round_endtime_epoch)
mkdir_cmd = 'mkdir -p ./data/{0}'.format(op_dir)
args = shlex.split(mkdir_cmd)
try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
    sys.exit(1)

    

op_s24_fname = './data/{0}/{1}_s24'.format(op_dir, reqd_asn)
op_s24_fp = open(op_s24_fname, 'w')
op_county_s24_fname = './data/{0}/{1}_county_s24'.format(op_dir, reqd_asn)
op_county_s24_fp = open(op_county_s24_fname, 'w')

# for s24 in dropout_s24s:
for s24 in s24_to_status:
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
        
        op_county_s24_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(s24, county_id, d, r, a) )

    op_s24_fp.write("{0}|{1}|{2}|{3}\n".format(s24, total_d, total_r, total_a ) )    


op_s24_set_fname = './data/{0}/{1}_s24_set'.format(op_dir, reqd_asn)
op_s24_set_fp = open(op_s24_set_fname, 'w')
for s24 in dropout_s24s:

    union_d = set()
    intersection_r = set()
    oct1, oct2, oct3, oct4_and_suf = split_ips(s24)
    for oct4 in range(256):
        intersection_r.add("{0}.{1}.{2}.{3}".format(oct1, oct2, oct3, oct4) )
    union_a = set()

    for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):
        if s24 in s24_to_status_set[roun]:
            if "r" in s24_to_status_set[roun][s24]:
                intersection_r = intersection_r & s24_to_status_set[roun][s24]['r']
            if "d" in s24_to_status_set[roun][s24]:
                union_d = union_d | s24_to_status_set[roun][s24]['d']
            if "a" in s24_to_status_set[roun][s24]:
                union_a = union_a | s24_to_status_set[roun][s24]['a']
        else:
            # If there were not even responsive addresses in this /24, then the set of addresses that responded across all these rounds contains 0 elements
            intersection_r = set()

    # Let's ensure that intersection_r contains no addresses from union_d
    intersection_r = intersection_r - union_d
            
    # op_s24_set_fp.write("{0}\t{1}|{2}|{3}\t{4}|{5}|{6}\t".format(s24, len(s24_to_status_set[0][s24]['d']), len(s24_to_status_set[0][s24]['r']), len(s24_to_status_set[0][s24]['a']), len(union_d), len(intersection_r), len(union_a) ) )            
    op_s24_set_fp.write("{0}\t|{1}|{2}|{3}\t".format(s24, len(union_d), len(intersection_r), len(union_a) ) )

    for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):
        # # We've already finished the 0th round
        # if roun == 0:
        #     continue

        if s24 not in s24_to_status_set[roun]:
            op_s24_set_fp.write("|0|0|0\t")
        else:
            op_s24_set_fp.write("|{0}|{1}|{2}\t".format(len(s24_to_status_set[roun][s24]['d']), len(s24_to_status_set[roun][s24]['r']), len(s24_to_status_set[roun][s24]['a']) ) )


    op_s24_set_fp.write("\n")
            
