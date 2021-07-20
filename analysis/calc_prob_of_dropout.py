
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

import sys
import datetime
from collections import defaultdict
import wandio

IS_COMPRESSED = 1

start_t = int(sys.argv[1])
end_t = int(sys.argv[2])

# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'
processed_op_dir = sys.argv[3]

addr_to_resps = defaultdict(int)
addr_to_dropouts = defaultdict(int)
addr_to_newresps = defaultdict(int)

resp_dropout_per_round_fname = '{0}/responsive_and_dropout_addrs/resp_dropout_per_round'.format(processed_op_dir)
resp_dropout_per_round_fp = open(resp_dropout_per_round_fname, 'w') # TODO: Maybe change to Append mode at some point? No, if I want to keep track of addr_to_* statistics

# all_addrs = set()

for this_t in range(start_t, end_t, 600):

    sys.stderr.write("\n\n{0}\n".format(str(datetime.datetime.now() ) ) )

    if IS_COMPRESSED == 1:
        ip_fname = '{0}/responsive_and_dropout_addrs/{1}_to_{2}.gz'.format(processed_op_dir, this_t, this_t + 600)
        sys.stderr.write("{0} is being processed\n".format(ip_fname) )
        
        try:
            ip_fp = wandio.open(ip_fname)
        except:
            continue

    else:
        ip_fname = '{0}/responsive_and_dropout_addrs/{1}_to_{2}'.format(processed_op_dir, this_t, this_t + 600)
        sys.stderr.write("{0} is being processed\n".format(ip_fname) )
    
        try:
            ip_fp = open(ip_fname)
        except IOError:
            continue

    n_r = 0
    n_d = 0
    n_n = 0 

    for line in ip_fp:
        parts = line.strip().split()
        addr = parts[0]

        # all_addrs.add(addr)

        is_resp = parts[1]

        if is_resp == '0':
            n_d += 1
            addr_to_dropouts[addr] += 1
        elif is_resp == '1':
            n_r += 1
            addr_to_resps[addr] += 1
        elif is_resp == '2':
            n_n += 1
            addr_to_newresps[addr] += 1

    resp_dropout_per_round_fp.write("{0} {1} {2} {3}\n".format(this_t, n_d, n_r, n_n) )
    resp_dropout_per_round_fp.flush()

    sys.stderr.write("{0}\n\n".format(str(datetime.datetime.now() ) ) )


# Write addr_to_droputs
if IS_COMPRESSED == 1:
    addr_to_dropouts_fname = '{0}/responsive_and_dropout_addrs/addr_to_dropouts.gz'.format(processed_op_dir)
    addr_to_dropouts_fp = wandio.open(addr_to_dropouts_fname, 'w')
else:
    addr_to_dropouts_fname = '{0}/responsive_and_dropout_addrs/addr_to_dropouts'.format(processed_op_dir)
    addr_to_dropouts_fp = open(addr_to_dropouts_fname, 'w')
    
for addr in addr_to_resps:
    addr_to_dropouts_fp.write("{0} {1} {2} {3}\n".format(addr, addr_to_dropouts[addr], addr_to_resps[addr], addr_to_newresps[addr]) )
addr_to_dropouts_fp.close()
    
# Key thing to note:
# Responsive addresses is the number of addresses that responded in the *previous* round. So the number of addresses that responded in a given round is #responsive + #newresps - #dropouts. 
# One way the above rule can be broken, is if an address was not pinged at all in a particular round.
# *Another* way the rule can be broken, is if pings to an address sent icmp errors in the previous round but pings to the address result in successful responses this round. Then we have more "responsive" addresses (as measured by awk '{if ($3 > 0) print}' resps_per_addr in a particular round),  but  they wouldn't be considered "newly responsive" according to this script here because an address is newly responsive only if it was completely unresponsive to pings in the previous round.
