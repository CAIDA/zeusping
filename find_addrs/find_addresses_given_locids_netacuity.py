
# For finding US addresses, run as:
# zcat /data/external/netacuity-dumps/Edge-processed/2019-01-18.netacq-4-blocks.csv.gz | python find_addresses_given_locids_netacuity.py us_locids  /scratch/satc/active-probing/trinarkular-validation/us_addrs/ all_us_addresses.gz
# TODO: Is it possible to make this program run faster?

# For finding San Diego addressesm run a:
# zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-blocks.latest.csv.gz | python find_addresses_given_locids_netacuity.py sandiego_locids /scratch/satc/active-probing/trinarkular-validation/us_addrs/sandiego_addrs/ all_sandiego_addresses.gz

import sys
import socket
import struct
import wandio
import os

def int2ip(addr):
    return socket.inet_ntoa(struct.pack("!I", addr))

us_locids = set()
us_locids_fp = open(sys.argv[1], 'r')
for line in us_locids_fp:
    us_locids.add(int(line[:-1]) )


op_path = sys.argv[2]
mkdir_cmd = 'mkdir -p {0}/'.format(op_path)
sys.stderr.write("{0}\n".format(mkdir_cmd) )
os.system(mkdir_cmd)

op_fname = sys.argv[3]
op_fp = wandio.open('{0}/{1}'.format(op_path, op_fname), 'w')
octets_op_fp = {}

oct3_reqd = 0

if oct3_reqd == 1:
    mkdir_cmd = 'mkdir -p {0}/addrs_by_oct3/'.format(op_path)
    sys.stderr.write("{0}\n".format(mkdir_cmd) )
    os.system(mkdir_cmd)

line_ct = 0
for line in sys.stdin:
    line_ct += 1

    if (line_ct == 1):
        continue
    
    parts = line.strip().split(',')

    locid = int(parts[2])

    if locid in us_locids:
        starting_ip_id = int(parts[0])
        ending_ip_id = int(parts[1])

        for ip in range(starting_ip_id, ending_ip_id+1):
            ip_str = int2ip(ip)
            op_fp.write("{0}\n".format(ip_str) )

            if oct3_reqd == 1:
                octs = ip_str.split('.')
                # oct1 = octs[0]
                oct3 = octs[3]            

                if oct3 not in octets_op_fp:
                    octets_op_fp[oct3] = wandio.open('{0}/addrs_by_oct3/{1}.gz'.format(op_path, oct3), 'w')

                octets_op_fp[oct3].write("{0}\n".format(ip_str) )
                

# Close all wandio files
op_fp.close()

if oct3_reqd == 1:
    for oct3 in octets_op_fp:
        octets_op_fp[oct3].close()
