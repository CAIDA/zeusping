
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

# 1. Identify a 10-minute round where an AS had a spike in dropouts
# 2. Identify each /24 of that AS
# 3. Identify each /24's counties and number of responsive, number of dropout addresses, number of anti-dropout addresses
# 4. For /24s with at least one dropout in this round: Also identify each /24's estimates of dropouts in the previous N rounds (d_prev[0, 1, 2, 3...]), dropouts in the next N rounds d_post[0, 1, 2, 3...]), all dropouts from previous N rounds to next N rounds (n_d_prev_to_post, which is a union) and set of addresses that were responsive from previous N rounds to next N rounds (n_r_prev_to_post, which is an intersection)

# Open previous file by subtracting 600 * N from start_time.

# NOTE NOTE: Dropouts and anti-dropouts are unions of addresses. Responsive addresses are intersections of addresses across *all* rounds. Thus, addresses that dropped out, or that had an anti-dropout, will never be responsive. Consider, for example an anti-dropout address in round -N. Since that address hadn't been considered "responsive" at the beginning of round -N (it hadn't been responsive in round -(N+1) and it would have a status of "anti-dropout" in round -N), that address will be absent from at least one round's responsive address set and will therefore not be a part of the union.

# NOTE: There was a bug in this code. We were investigating rda files for rounds (T-1, T, T+1), therefore investigating responses in rounds T-2, T-1, T. This was correct for dropouts but incorrect for responses.
# We fixed this bug by reading responses from rda files for rounds (T, T+1, T+2) and dropouts from rda files for rounds (T-1, T, T+1)


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
    parts = line.strip().split('|')
    ip = parts[0].strip()
    asn = parts[4].strip()
    loc1 = parts[5].strip()
    loc2 = parts[6].strip()
    loc2_name = parts[7].strip()
    
    ip_to_metadata[ip] = {"asn" : asn, "loc1_id" : loc1, "loc2_id" : loc2, "loc2_name" : loc2_name}


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
for roun in range(-(num_adjacent_rounds+1), (num_adjacent_rounds+2) ):
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
                
            # loc2_id = ip_to_metadata[addr]["loc2_id"]
            loc2_name = ip_to_metadata[addr]["loc2_name"]
            
            # if loc2_id not in s24_to_status[s24]:
            if loc2_name not in s24_to_status[s24]:
                # s24_to_status[s24][loc2_id] = {"r" : 0, "d" : 0, "a" : 0}                
                s24_to_status[s24][loc2_name] = {"r" : 0, "d" : 0, "a" : 0}
                
            # s24_to_status[s24][loc2_id][status] += 1
            s24_to_status[s24][loc2_name][status] += 1

            # Just update all the s24_to_status_set in the next loop
            # if s24 not in s24_to_status_set[0]:
            #     s24_to_status_set[0][s24] =  {"d" : set(), "r" : set(), "a": set()}
            # s24_to_status_set[0][s24][status].add(addr)

            if status == 'd':
                dropout_s24s.add(s24)


for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+2) ):

    # We've already finished the 0th round
    # if roun == 0:
    #     continue
    
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

            if status == 'd' or status == 'a':
                
                if s24 not in s24_to_status_set[roun]:
                    s24_to_status_set[roun][s24] =  {"d" : set(), "r" : set(), "a": set()}

                s24_to_status_set[roun][s24][status].add(addr)

            # rda files indicate responsiveness at the beginning of a round (i.e., in the previous round). So update the previous round.
            elif status == 'r':
                if s24 not in s24_to_status_set[roun-1]:
                    s24_to_status_set[roun-1][s24] =  {"d" : set(), "r" : set(), "a": set()}

                s24_to_status_set[roun-1][s24][status].add(addr)
                
                
                
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
op_loc2_s24_fname = './data/{0}/{1}_loc2_s24'.format(op_dir, reqd_asn)
op_loc2_s24_fp = open(op_loc2_s24_fname, 'w')

# for s24 in dropout_s24s:
for s24 in s24_to_status:
    total_r = 0
    total_d = 0
    total_a = 0
    for loc2_id in s24_to_status[s24]:
        r = s24_to_status[s24][loc2_id]["r"]
        d = s24_to_status[s24][loc2_id]["d"]
        a = s24_to_status[s24][loc2_id]["a"]
        total_r += r
        total_d += d
        total_a += a
        
        op_loc2_s24_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(s24, loc2_id, d, r, a) )

    op_s24_fp.write("{0}|{1}|{2}|{3}\n".format(s24, total_d, total_r, total_a ) )    


op_s24_set_fname = './data/{0}/{1}_s24_set'.format(op_dir, reqd_asn)
op_s24_set_fp = open(op_s24_set_fname, 'w')
op_s24_set_details_fname = './data/{0}/{1}_s24_set_details'.format(op_dir, reqd_asn)
op_s24_set_details_fp = open(op_s24_set_details_fname, 'w')

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

    # if s24 == '74.77.164.0/24':
    #     print "Before subtracting union_d: {0}".format(len(intersection_r) )
            
    # Let's ensure that intersection_r contains no addresses from union_d
    intersection_r = intersection_r - union_d

    # if s24 == '74.77.164.0/24':
    #     print "After subtracting union_d: {0}".format(len(intersection_r) )
    
    # op_s24_set_fp.write("{0}\t{1}|{2}|{3}\t{4}|{5}|{6}\t".format(s24, len(s24_to_status_set[0][s24]['d']), len(s24_to_status_set[0][s24]['r']), len(s24_to_status_set[0][s24]['a']), len(union_d), len(intersection_r), len(union_a) ) )            
    op_s24_set_fp.write("{0}\t|{1}|{2}|{3}\t".format(s24, len(union_d), len(intersection_r), len(union_a) ) )

    op_s24_set_details_fp.write("{0}\t".format(s24) )
    
    for addr in union_d:
        op_s24_set_details_fp.write("{0}-".format(addr) )
    op_s24_set_details_fp.write("|")

    for addr in intersection_r:
        op_s24_set_details_fp.write("{0}-".format(addr) )
    op_s24_set_details_fp.write("|")

    for addr in union_a:
        op_s24_set_details_fp.write("{0}-".format(addr) )
    op_s24_set_details_fp.write("|")

    for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):
        # # We've already finished the 0th round
        # if roun == 0:
        #     continue

        if s24 not in s24_to_status_set[roun]:
            op_s24_set_fp.write("|0|0|0\t")

            op_s24_set_details_fp.write("|||\t")
            
        else:
            op_s24_set_fp.write("|{0}|{1}|{2}\t".format(len(s24_to_status_set[roun][s24]['d']), len(s24_to_status_set[roun][s24]['r']), len(s24_to_status_set[roun][s24]['a']) ) )

            for addr in s24_to_status_set[roun][s24]['d']:
                op_s24_set_details_fp.write("{0}-".format(addr) )
            op_s24_set_details_fp.write("|")

            for addr in s24_to_status_set[roun][s24]['r']:
                op_s24_set_details_fp.write("{0}-".format(addr) )
            op_s24_set_details_fp.write("|")

            for addr in s24_to_status_set[roun][s24]['a']:
                op_s24_set_details_fp.write("{0}-".format(addr) )
            op_s24_set_details_fp.write("|")

    op_s24_set_fp.write("\n")
    op_s24_set_details_fp.write("\n")        
