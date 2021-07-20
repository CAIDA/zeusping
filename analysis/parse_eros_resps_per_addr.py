
#  This software is Copyright (c) 2019 The Regents of the University of
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

# For each address: find how many pings were sent and how many were responsive.
import sys
import os
import json
import collections
from operator import *
import wandio

# @profile
def update_addr_to_resps(addr_to_resps):

    for line in sys.stdin:

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

# @profile    
def write_addr_to_resps(addr_to_resps):
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

    
addr_to_resps = {}

update_addr_to_resps(addr_to_resps)

write_addr_to_resps(addr_to_resps)

