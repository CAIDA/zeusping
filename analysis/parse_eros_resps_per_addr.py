
import sys
import os
import json
import collections
from operator import *
import wandio

# For each address: find how many pings were sent and how many were responsive.


addr_to_resps = {}
line_ct = 0


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

    if dst not in addr_to_resps:
        addr_to_resps[dst] = [0, 0, 0, 0, 0]
    addr_to_resps[dst][0] += 1 # 0th index is sent packets

    # pinged_ts = data['start']['sec']

    resps = data['responses']

    if resps: # Apparently this way of checking for elements in a list is much faster than checking len
        this_resp = resps[0]
        icmp_type = this_resp["icmp_type"]
        icmp_code = this_resp["icmp_code"]
        
        if icmp_type == 0 and icmp_code == 0:
            # Responded to the ping and response is indicative of working connectivity
            addr_to_resps[dst][1] += 1 # 1st index is successful ping response
        elif icmp_type == 3 and icmp_code == 1:
            # Destination host unreachable
            addr_to_resps[dst][2] += 1 # 2nd index is Destination host unreachable
        else:
            addr_to_resps[dst][3] += 1 # 3rd index is the rest of icmp stuff. So mostly errors.

    else:
        
        addr_to_resps[dst][4] += 1 # 4th index is lost ping

    # is_loss = data['statistics']['loss']

    

if 'gz' in sys.argv[1]:
    ping_aggrs_fp = wandio.open(sys.argv[1], 'w')
else:
    ping_aggrs_fp = open(sys.argv[1], 'w')


dst_ct = 0
for dst in addr_to_resps:

    dst_ct += 1
    this_d = addr_to_resps[dst]
    ping_aggrs_fp.write("{0} {1} {2} {3} {4} {5}\n".format(dst, this_d[0], this_d[1], this_d[2], this_d[3], this_d[4] ) )
    # ping_aggrs_fp.write("{0} {1} {2} 0\n".format(dst, this_d[0], this_d[1]) )
    
    # if dst_ct == 1:
    #     sys.exit(1)


ping_aggrs_fp.close()
