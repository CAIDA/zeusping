
# Ideally, we would specify a round_tstart and a round_tend.
# Then for each address that experienced a dropout in the round_tstart +- adjacent rounds, we would list the rounds when addresses recovered. Rounds where the most addresses recovered *may* be signify the end of the outage. Caveats: If the recoveries are distributed across lots of non-adjacent rounds, suggestive that the addresses simply went back into the provider's DHCP pool.

import sys
# import pyipmeta
from collections import namedtuple
from collections import defaultdict
import wandio
import datetime
import dateutil
from dateutil.parser import parse
import shlex
import subprocess

inp_path = sys.argv[1]
round_tstart = int(sys.argv[2])
round_tend = int(sys.argv[3])
reqd_asn = sys.argv[4]
addr_metadata_fname = sys.argv[5]
num_adjacent_rounds = int(sys.argv[6])
is_compressed = int(sys.argv[7])

ROUND_SECS = 10 * 60 # Number of seconds in a 10-minute round

ip_to_metadata = {}
if is_compressed == 1:
    addr_metadata_fp = wandio.open(addr_metadata_fname)
else:
    addr_metadata_fp = open(addr_metadata_fname)
for line in addr_metadata_fp:
    parts = line.strip().split()
    ip = parts[0].strip()
    asn = parts[4].strip()
    county_id = parts[5].strip()
    
    county_name = ''
    for elem in parts[6:]:
        county_name += '{0} '.format(elem)

    ip_to_metadata[ip] = {"asn" : asn, "county_id" : county_id, "county_name" : county_name}


# Begin by identifying all the addresses that dropped out in this (and adjacent) rounds belonging to reqd_asn
dropout_addrs = set()
for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):
    
    if is_compressed == 1:
        inp_fname = "{0}/{1}_to_{2}.gz".format(inp_path, round_tstart, round_tstart + ROUND_SECS )
        inp_fp = wandio.open(inp_fname)
    else:
        inp_fname = "{0}/{1}_to_{2}".format(inp_path, round_tstart, round_tstart + ROUND_SECS )
        inp_fp = open(inp_fname)

    for line in inp_fp:
        parts = line.strip().split()

        if len(parts) != 2:
            sys.stderr.write("Weird line length: {0}\n".format(line) )
            sys.exit(1)

        addr = parts[0].strip()
        status = parts[1].strip()

        if status == '0':
            status = 'd'
            
            if addr in ip_to_metadata:
                if ip_to_metadata[addr]["asn"] == reqd_asn:
                    dropout_addrs.add(addr)
                


inp_fname_parts = inp_fname.strip().split('_')
if is_compressed == 1:
    round_end_time_epoch = int(inp_fname_parts[-1][:-3])
else:
    round_end_time_epoch = int(inp_fname_parts[-1])
round_start_time_epoch = round_end_time_epoch - ROUND_SECS

this_h_dt = datetime.datetime.utcfromtimestamp(round_start_time_epoch)
this_h_dt_str = this_h_dt.strftime("%Y_%m_%d_%H_%M")


op_dir = '{0}_{1}_to_{2}'.format(this_h_dt_str, round_start_time_epoch, round_end_time_epoch)
mkdir_cmd = 'mkdir -p ./data/{0}'.format(op_dir)
args = shlex.split(mkdir_cmd)
try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
    sys.exit(1)

dropout_addrs_fname = './data/{0}/{1}_adjrounds{2}_dropoutaddrs'.format(op_dir, reqd_asn, num_adjacent_rounds)
dropout_addrs_fp = open(dropout_addrs_fname, 'w')
for addr in dropout_addrs:
    dropout_addrs_fp.write("{0}\n".format(addr) )
    

round_to_recoveries = defaultdict(set)
# Next, go through each round from round_tstart+1 to round_tend. See how many of the dropped out addresses recovered in each round.
this_round_tstart = round_tstart + ROUND_SECS
while (this_round_tstart <= round_tend):

    if is_compressed == 1:
        temp_inp_fname = "{0}/{1}_to_{2}.gz".format(inp_path, this_round_tstart, this_round_tstart + ROUND_SECS)
        temp_inp_fp = wandio.open(temp_inp_fname)
    else:
        temp_inp_fname = "{0}/{1}_to_{2}".format(inp_path, this_round_tstart, this_round_tstart + ROUND_SECS)
        temp_inp_fp = open(temp_inp_fname)

    sys.stderr.write("Working on analyzing recoveries in {0}\n".format(temp_inp_fname) )

    for line in temp_inp_fp:
        parts = line.strip().split()

        if len(parts) != 2:
            sys.stderr.write("Weird line length: {0}\n".format(line) )
            sys.exit(1)

        addr = parts[0].strip()
        status = parts[1].strip()

        if status == '2':
            status = 'a'

            if addr in dropout_addrs:
                round_to_recoveries[this_round_tstart].add(addr)
                # Since we only care about the first recovery of an address, remove this address from dropout_addrs
                dropout_addrs.remove(addr)
    
    # Update while loop variable
    this_round_tstart += ROUND_SECS


recs_per_round_fname = './data/{0}/{1}_adjrounds{2}_recsperround'.format(op_dir, reqd_asn, num_adjacent_rounds)
recs_per_round_fp = open(recs_per_round_fname, 'w')
recaddrs_per_round_fname = './data/{0}/{1}_adjrounds{2}_recaddrsperround'.format(op_dir, reqd_asn, num_adjacent_rounds)
recaddrs_per_round_fp = open(recaddrs_per_round_fname, 'w')
for roun in round_to_recoveries:
    recs_per_round_fp.write("{0} {1}\n".format(roun, len(round_to_recoveries[roun]) ) )

    addr_str = ''
    for addr in round_to_recoveries[roun]:
        addr_str += '{0}|'.format(addr)

    addr_str = addr_str[:-1]

    recaddrs_per_round_fp.write("{0} {1} {2}\n".format(roun, len(round_to_recoveries[roun]), addr_str ) )
    
