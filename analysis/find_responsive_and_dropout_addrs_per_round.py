
# Conceptually, at the beginning of a round t, there are N (say 1000) addresses that responded in the last round and can potentially dropout this round. Let's suppose there are D dropouts (say 100), then 950 of them continued to respond in this round as well (NOTE: the way erosprober works, if an address hasn't dropped out, then it *must* be responsive since all addresses are pinged each round). However, it is possible that some addresses dropped out but it was just reassignment or something so M other addresses (say 50) lit up. Then the total number of outages is D - M. 

import sys
import os
import datetime

processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'

this_t = int(sys.argv[1])

# First get responsive addresses using the previous round. These are all the addresses that have the potential to fail in this round.

prev_t = this_t - 600
prev_t_file = '{0}/temp_{1}_to_{2}/resps_per_addr'.format(processed_op_dir, prev_t, this_t)
sys.stderr.write("Prev file: {0}\n".format(prev_t_file) )
prev_t_fp = open(prev_t_file, 'r')

unresp_addrs = set() # These are the set of unresponsive addresses at the beginning of this_t (i.e., they were unresponsive in the previous round)
resp_addrs = set() # These are the set of responsive addresses at the beginning of this_t (i.e., they were responsive in the previous round)

for line in prev_t_fp:
    parts = line.strip().split()
    addr = parts[0]

    sent_pkts = int(parts[1])
    successful_resps = int(parts[2])
    host_unreach = int(parts[3])
    icmp_err = int(parts[4])
    losses = int(parts[5])

    # TODO: Think about whether I should use host_unreach and icmp_err messages in some way
    if successful_resps > 0:
        resp_addrs.add(addr) # This address was responsive in the previous round and has the potential to fail
    elif losses == sent_pkts:
        unresp_addrs.add(addr) # This address was completely unresponsive in the previous round


# print len(unresp_addrs)

this_t_file = '{0}/temp_{1}_to_{2}/resps_per_addr'.format(processed_op_dir, this_t, this_t + 600)
sys.stderr.write("This file: {0}\n".format(this_t_file) )
this_t_fp = open(this_t_file, 'r')

dropout_addrs = set()
newresp_addrs = set()

for line in this_t_fp:
    parts = line.strip().split()

    addr = parts[0]

    sent_pkts = int(parts[1])
    successful_resps = int(parts[2])
    host_unreach = int(parts[3])
    icmp_err = int(parts[4])
    losses = int(parts[5])

    # If all pings sent in this round were lost and this address was responsive at the beginning of this round, then this address experienced a dropout.
    if losses == sent_pkts and addr in resp_addrs:
        dropout_addrs.add(addr)

    # If the address had been completely unresponsive last round but is responsive now, then this is a newly responsive address.
    if successful_resps > 0 and addr in unresp_addrs:
        newresp_addrs.add(addr)

# print len(newresp_addrs)

mkdir_cmd = 'mkdir -p {0}/responsive_and_dropout_addrs/'.format(processed_op_dir)
os.system(mkdir_cmd)

op_fname = '{0}/responsive_and_dropout_addrs/{1}_to_{2}'.format(processed_op_dir, this_t, this_t + 600)
op_fp = open(op_fname, 'w')

for addr in dropout_addrs:
    op_fp.write("{0} 0\n".format(addr) )

for addr in resp_addrs:
    # if addr not in dropout_addrs:
        op_fp.write("{0} 1\n".format(addr) )

for addr in newresp_addrs:
    op_fp.write("{0} 2\n".format(addr) )
