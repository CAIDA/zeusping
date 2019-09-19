
import sys
import datetime
from collections import defaultdict

processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'

start_t = int(sys.argv[1])
end_t = int(sys.argv[2])

addr_to_resps = defaultdict(int)
addr_to_dropouts = defaultdict(int)
addr_to_newresps = defaultdict(int)

resp_dropout_per_round_fname = '{0}/responsive_and_dropout_addrs/resp_dropout_per_round'.format(processed_op_dir)
resp_dropout_per_round_fp = open(resp_dropout_per_round_fname, 'w') # TODO: Maybe change to Append mode at some point?

# all_addrs = set()

for this_t in range(start_t, end_t, 600):

    sys.stderr.write("\n\n{0}\n".format(str(datetime.datetime.now() ) ) )
    ip_fname = '{0}/responsive_and_dropout_addrs/{1}_to_{2}'.format(processed_op_dir, this_t, this_t + 600)
    sys.stderr.write("{0} is being processed\n".format(ip_fname) )

    ip_fp = open(ip_fname)

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

    resp_dropout_per_round_fp.write("{0} {1} {2} {3}\n".format(this_t, n_r, n_n, n_d) )
    resp_dropout_per_round_fp.flush()

    sys.stderr.write("{0}\n\n".format(str(datetime.datetime.now() ) ) )


# Write addr_to_droputs
addr_to_dropouts_fname = '{0}/responsive_and_dropout_addrs/addr_to_dropouts'.format(processed_op_dir)
addr_to_dropouts_fp = open(addr_to_dropouts_fname, 'w')
for addr in addr_to_resps:
    addr_to_dropouts_fp.write("{0} {1} {2} {3}\n".format(addr, addr_to_resps[addr], addr_to_newresps[addr], addr_to_dropouts[addr]) )

# Key thing to note:
# Responsive addresses is the number of addresses that responded in the *previous* round. So the number of addresses that responded in a given round is #responsive + #newresps - #dropouts. 
# The only way the above invariant can be broken, is if an address was not pinged at all in a particular round.
