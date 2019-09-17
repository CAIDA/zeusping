
# TODO: I think Dst unreachable responses need to be ignored as well. 

import sys
import os
import json
import collections
from operator import *

# For each address: find how many pings were sent and how many were responsive. 

addr_to_resps = {}
line_ct = 0


def find_num_successful_pings(sorted_resps):
    num_total = 0
    num_succ = 0
    for resp in sorted_resps:
        if resp.responsive == 1:
            num_succ += 1
        num_total += 1

    return num_succ, num_total


for line in sys.stdin:

    line_ct += 1
    
    data = json.loads(line)

    # if 'statistics' not in data:
    #     print data

    # if 'dst' not in data:
    #     print data

    # if 'start' not in data:
    #     print data
    
    dst = data['dst']
    pinged_ts = data['start']['sec']

    unresponsive = data['statistics']['loss']

    if unresponsive == 1:
        responsive = 0
    else:
        responsive = 1

    if dst not in addr_to_resps:
        addr_to_resps[dst] = [0, 0]

    addr_to_resps[dst][0] += 1 # 0th index is sent packets
    if responsive == 1:
        addr_to_resps[dst][1] += 1 # 1st index is responsive packets

    
ping_aggrs_fp = open(sys.argv[1], 'w')


dst_ct = 0
for dst in addr_to_resps:

    dst_ct += 1
    this_d = addr_to_resps[dst]
    if this_d[1] > 0:
        ping_aggrs_fp.write("{0} {1} {2} 1\n".format(dst, this_d[0], this_d[1]) )
    else:
        ping_aggrs_fp.write("{0} {1} {2} 0\n".format(dst, this_d[0], this_d[1]) )
    
    # if dst_ct == 1:
    #     sys.exit(1)


