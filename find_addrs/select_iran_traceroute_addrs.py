

import sys
import random
from collections import defaultdict

op_fname = sys.argv[1]


op_fp = open(op_fname, 'w')

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


for asn in asn_to_addrs:

    if asn in top_20_asns:

        num_addrs_asn = asn_to_tot_addrs[asn]

        this_asn_share = 1000 * num_addrs_asn/float(top_20_tot_addrs)

        this_asn_addrs = asn_to_addrs[asn]
        random.shuffle(this_asn_addrs)

        written_ct = 0
        for addr in this_asn_addrs:

            if written_ct > this_asn_share:
                break
            
            op_fp.write("{0}|{1}\n".format(addr, asn) )

            written_ct += 1
    

# for asn in asn_to_addrs:
#     for addr in asn_to_addrs[asn]:
#         num = random.randint(1, 2)

#         if num == 2:
#             op_fp.write("{0}\n".format(addr) )

        
    
    
