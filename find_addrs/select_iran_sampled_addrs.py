

import sys
import random
import math
from collections import defaultdict

op_fname_pref = sys.argv[1]
num_samples = int(sys.argv[2])
# op_fp = open(op_fname, 'w')

asn_to_addrs = {}
asn_to_tot_addrs = defaultdict(int)

line_ct = 0
tot_addrs = 0

for line in sys.stdin:

    line_ct += 1

    if line_ct % 100000 == 0:
        sys.stderr.write("Lines processed: {0}\n".format(line_ct) )
    
    parts = line.strip().split('|')

    if (len(parts) != 2):
        continue
    
    addr = parts[0].strip()
    asn = parts[1].strip()

    if asn not in asn_to_addrs:
        asn_to_addrs[asn] = []
    asn_to_addrs[asn].append(addr)

    asn_to_tot_addrs[asn] += 1

    tot_addrs += 1


top_20_tot_addrs = 0
sorted_tuple = sorted(asn_to_tot_addrs.items(), key=lambda kv: kv[1], reverse=True)


top_20_asns = set()
num_asns_done = 0
for entry in sorted_tuple:

    if num_asns_done == 20:
        break

    asn = entry[0]

    top_20_asns.add(asn)
    
    num_addrs = entry[1]

    # print asn, num_addrs
    
    top_20_tot_addrs += num_addrs

    num_asns_done += 1

print top_20_tot_addrs


# def split_ips(ip):
#     parts = ip.strip().split('.')
#     oct1 = parts[0].strip()
#     oct2 = parts[1].strip()
#     oct3 = parts[2].strip()
#     oct4 = parts[3].strip()

#     return oct1, oct2, oct3, oct4


# def find_s24(ipv4_addr):
#     oct1, oct2, oct3, oct4 = split_ips(ipv4_addr)
#     return "{0}.{1}.{2}.0/24".format(oct1, oct2, oct3)

op_fps = {}
for file_idx in range(num_samples):
    op_fps[file_idx] = open('./data/{0}_{1}samples_s{2}_unsorted'.format(op_fname_pref, num_samples, file_idx+1), 'w')
    
    
for asn in asn_to_addrs:

    num_addrs_asn = asn_to_tot_addrs[asn]

    # num_addrs_asn_per_file = math.ceil(num_addrs_asn/float(num_samples)) # If we have 9 addresses in an AS and 4 files, we will obtain ceil(9/4) = 3 addrs per file. So first 3 addrs go into file 1, next 3 addrs go into file 2, next 3 go into file 3 and nothing into file 4!    
    # num_addrs_asn_per_file = num_addrs_asn/float(num_samples) # This didn't solve the above problem; in fact the outputs were identical (of course)!

    # this_asn_share = 1000 * num_addrs_asn/float(top_20_tot_addrs)
    this_asn_addrs = asn_to_addrs[asn]
    random.shuffle(this_asn_addrs)

    # file_idx = 0
    written_ct = 0

    for addr in this_asn_addrs:

        # if written_ct >= num_addrs_asn_per_file:
        #     file_idx += 1
        #     written_ct = 0

        file_idx = written_ct%num_samples

        op_fps[file_idx].write("{0}|{1}\n".format(addr, asn) )
        written_ct += 1
    
