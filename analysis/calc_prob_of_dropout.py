
import sys
import datetime
from collections import defaultdict

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
addr_to_dropouts_fname = '{0}/responsive_and_dropout_addrs/addr_to_dropouts'.format(processed_op_dir)
addr_to_dropouts_fp = open(addr_to_dropouts_fname, 'w')
for addr in addr_to_resps:
    addr_to_dropouts_fp.write("{0} {1} {2} {3}\n".format(addr, addr_to_dropouts[addr], addr_to_resps[addr], addr_to_newresps[addr]) )

# Key thing to note:
# Responsive addresses is the number of addresses that responded in the *previous* round. So the number of addresses that responded in a given round is #responsive + #newresps - #dropouts. 
# One way the above rule can be broken, is if an address was not pinged at all in a particular round.
# *Another* way the rule can be broken, is if pings to an address sent icmp errors in the previous round but pings to the address result in successful responses this round. Then we have more "responsive" addresses (as measured by awk '{if ($3 > 0) print}' resps_per_addr in a particular round),  but  they wouldn't be considered "newly responsive" according to this script here because an address is newly responsive only if it was completely unresponsive to pings in the previous round.
