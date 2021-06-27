#!/usr/bin/env python3

# This script uses processed ping responses from round t-1 and round t to calculate the number of dropouts, responsive, and antidropout addresses per round
# Conceptually, at the beginning of a round t, there are N (say 1000) addresses that responded in round t-1 and can potentially dropout this round. Let's suppose there are D dropouts (say 100), then 900 of them continued to respond in this round as well (NOTE: the way erosprober works, if an address hasn't dropped out, then it *must* be responsive since all addresses are pinged each round). However, it is possible that some addresses dropped out due to reassignment so M *other* addresses (say 50) lit up. Then the total number of outages is D - M. 

import sys
import os
import datetime
import subprocess
import os
import wandio
import struct
import socket
import ctypes
import shlex
import gmpy
import gc
from collections import defaultdict

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
    from sc_warts import WartsReader
else:
    py_ver = 3


def setup_stuff(processed_op_dir, this_t, this_t_round_end):
    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}/'.format(processed_op_dir, this_t, this_t_round_end)
    args = shlex.split(mkdir_cmd)
    if py_ver == 2:
        try:
            subprocess32.check_call(args)
        except subprocess32.CalledProcessError:
            sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
            sys.exit(1)

    else:
        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
            sys.exit(1)

            
def get_fp(processed_op_dir, reqd_t, read_bin, is_swift, called_from):

    if read_bin == 1:
        reqd_t_file = '{0}/{1}_to_{2}/resps_per_round.gz'.format(processed_op_dir, reqd_t, reqd_t + ROUND_SECS)
        reqd_t_fp = wandio.open(reqd_t_file, 'rb')
    
    else:
            
        if IS_COMPRESSED == 1:
            reqd_t_file = '{0}/{1}_to_{2}/resps_per_addr.gz'.format(processed_op_dir, reqd_t, reqd_t + ROUND_SECS)
            reqd_t_fp = wandio.open(reqd_t_file, 'r')
        else:
            reqd_t_file = '{0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, reqd_t + ROUND_SECS)
            reqd_t_fp = open(reqd_t_file, 'r')

    sys.stderr.write("{0}: {1}\n".format(called_from, reqd_t_file) )

    return reqd_t_fp

            
def get_resp_unresp(processed_op_dir, this_t, read_bin, is_swift, unresp_addrs, resp_addrs):

    prev_t = this_t - ROUND_SECS

    prev_t_fp = get_fp(processed_op_dir, prev_t, read_bin, is_swift, "Prev")

    done_ct = 0
    
    while True:
        
    # for line in prev_t_fp:

        if read_bin == 1:
            data_chunk = prev_t_fp.read(struct_fmt.size * 2000)

            if len(data_chunk) == 0:
                break

            gen = struct_fmt.iter_unpack(data_chunk)

            for elem in gen:

                done_ct += 1
                
                ipid, sent_pkts, successful_resps, host_unreach, icmp_err, losses = elem
                # ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

                # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent_pkts, successful_resps, host_unreach, icmp_err, losses) )
                # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, gmpy.popcount(sent_pkts), gmpy.popcount(successful_resps), gmpy.popcount(host_unreach), gmpy.popcount(icmp_err), gmpy.popcount(losses) ) )

                # if done_ct == 100:
                #     print(resp_addrs)
                #     print(unresp_addrs)
                #     sys.exit(1)

                # if ipstr == '67.181.30.249':
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent_pkts, successful_resps, host_unreach, icmp_err, losses) )
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, gmpy.popcount(sent_pkts), gmpy.popcount(successful_resps), gmpy.popcount(host_unreach), gmpy.popcount(icmp_err), gmpy.popcount(losses) ) )
                    # sys.exit(1)
                
                if gmpy.popcount(successful_resps) > 0:
                    # resp_addrs.add(ipstr)
                    resp_addrs.add(ipid)
                # NOTE: In future, let's consider special cases where there may only have been a single active vantage point. 
                elif ( (gmpy.popcount(losses) == gmpy.popcount(sent_pkts) ) and (gmpy.popcount(successful_resps) == 0) ):
                    # unresp_addrs.add(ipstr)
                    unresp_addrs.add(ipid)

            data_chunk = ''

        else:
            line = prev_t_fp.readline()

            if not line:
                break
        
            parts = line.strip().split()
            
            ipstr = parts[0]
            ipid = struct.unpack("!I", socket.inet_aton(ipstr))[0]

            sent_pkts = int(parts[1])
            successful_resps = int(parts[2])
            host_unreach = int(parts[3])
            icmp_err = int(parts[4])
            losses = int(parts[5])

            # TODO: Think about whether I should use host_unreach and icmp_err messages in some way
            if successful_resps > 0:
                resp_addrs.add(ipid) # This address was responsive in the previous round and has the potential to fail
            elif losses == sent_pkts:
                unresp_addrs.add(ipid) # This address was completely unresponsive in the previous round

    prev_t_fp.close()


def get_dropout_antidropout(processed_op_dir, this_t, read_bin, is_swift, unresp_addrs, resp_addrs, addr_to_status, resp_addrs_this_round=None):

    this_t_fp = get_fp(processed_op_dir, this_t, read_bin, is_swift, "This")

    if read_bin == 1:

        while True:
        
            data_chunk = this_t_fp.read(struct_fmt.size * 2000)

            if len(data_chunk) == 0:
                break

            gen = struct_fmt.iter_unpack(data_chunk)

            for elem in gen:

                ipid, sent_pkts, successful_resps, host_unreach, icmp_err, losses = elem
                # ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

                # if ipstr == '67.181.30.249':
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent_pkts, successful_resps, host_unreach, icmp_err, losses) )
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, gmpy.popcount(sent_pkts), gmpy.popcount(successful_resps), gmpy.popcount(host_unreach), gmpy.popcount(icmp_err), gmpy.popcount(losses) ) )
                #     sys.exit(1)

                # NOTE: The (gmpy.popcount(successful_resps) == 0) test is for unusual cases where a vantage point may have pinged an address more than once in a round and one of the responses was a loss but the other was successful. For a dropout, we want *all* pings to be unresponsive in this round.
                if ( (gmpy.popcount(losses) == gmpy.popcount(sent_pkts) ) and (ipid in resp_addrs) and (gmpy.popcount(successful_resps) == 0) ):
                    # dropout_addrs.add(ipstr)
                    addr_to_status[ipid] = 0
                    # dropout_addrs.add(ipid)

                elif ( (gmpy.popcount(successful_resps) > 0) and (ipid in unresp_addrs) ):
                    # antidropout_addrs.add(ipstr)
                    addr_to_status[ipid] = 2
                    # antidropout_addrs.add(ipid)

                if resp_addrs_this_round != None:
                    if gmpy.popcount(successful_resps) > 0:
                        resp_addrs_this_round.add(ipid)

                    
    else:
        
        for line in this_t_fp:
            parts = line.strip().split()

            ipstr = parts[0]
            ipid = struct.unpack("!I", socket.inet_aton(ipstr))[0]

            sent_pkts = int(parts[1])
            successful_resps = int(parts[2])
            host_unreach = int(parts[3])
            icmp_err = int(parts[4])
            losses = int(parts[5])

            # If all pings sent in this round were lost and this address was responsive at the beginning of this round, then this address experienced a dropout.
            if losses == sent_pkts and ipid in resp_addrs:
                addr_to_status[ipid] = 0
                # dropout_addrs.add(ipstr)

            # If the address had been completely unresponsive last round but is responsive now, then this is a newly responsive address.
            elif successful_resps > 0 and ipid in unresp_addrs:
                addr_to_status[ipid] = 2
                # antidropout_addrs.add(ipstr)

            if resp_addrs_this_round != None:
                if successful_resps > 0:
                    resp_addrs_this_round.add(ipid)

    this_t_fp.close()

            
def write_op(processed_op_dir, this_t, this_t_round_end, addr_to_status, resp_addrs):

    if IS_COMPRESSED == 1:
        rda_op_fname = '{0}/{1}_to_{2}/rda.gz'.format(processed_op_dir, this_t, this_t_round_end)
        rda_op_fp = wandio.open(rda_op_fname, 'w')
    else:
        rda_op_fname = '{0}/{1}_to_{2}/rda'.format(processed_op_dir, this_t, this_t_round_end)
        rda_op_fp = open(rda_op_fname, 'w')

    this_roun_addr_to_status = addr_to_status[0]

    to_write_addr_set = this_roun_addr_to_status.keys() | resp_addrs[-1]

    for addr in sorted(to_write_addr_set):
        
        # if read_bin == 1:
        ipstr = socket.inet_ntoa(struct.pack('!L', addr))
        # else:
        #     ipstr = addr
        
        if addr in this_roun_addr_to_status:
            if this_roun_addr_to_status[addr] == 0:
                rda_op_fp.write("{0} 1\n".format(ipstr) ) # The address was responsive at the beginning of the round
                rda_op_fp.write("{0} 0\n".format(ipstr) ) # The address experienced a dropout this round
            else:
                rda_op_fp.write("{0} 2\n".format(ipstr) ) # The address experienced an anti-dropout this round

        else:
            rda_op_fp.write("{0} 1\n".format(ipstr) ) # The address was responsive at the beginning of the round

    # for addr in this_roun_addr_to_status:
    #     if read_bin == 1:
    #         ipstr = socket.inet_ntoa(struct.pack('!L', addr))
    #     else:
    #         ipstr = addr
    #     rda_op_fp.write("{0} {1}\n".format(ipstr, this_roun_addr_to_status[addr]) )

    # for addr in resp_addrs[-1]:
    #     if read_bin == 1:
    #         ipstr = socket.inet_ntoa(struct.pack('!L', addr))
    #     else:
    #         ipstr = addr
    #     # if addr not in dropout_addrs:
    #     rda_op_fp.write("{0} 1\n".format(ipstr) )

    rda_op_fp.close() # wandio does not like it if the fp is not closed explicitly

    if IS_COMPRESSED == 1:
        rda_multiround_op_fname = '{0}/{1}_to_{2}/rda_multiround.gz'.format(processed_op_dir, this_t, this_t_round_end)
        rda_multiround_op_fp = wandio.open(rda_multiround_op_fname, 'w')
    else:
        rda_multiround_op_fname = '{0}/{1}_to_{2}/rda_multiround'.format(processed_op_dir, this_t, this_t_round_end)
        rda_multiround_op_fp = open(rda_multiround_op_fname, 'w')

    resp_all_rounds_addr_set = resp_addrs[-1] & resp_addrs[0] & resp_addrs[1]
    to_write_addr_set = addr_to_status[-1].keys() | this_roun_addr_to_status.keys() | addr_to_status[1].keys() | resp_all_rounds_addr_set

    # addr_to_multiround_status = {}
    for addr in sorted(to_write_addr_set):
        
        # if read_bin == 1:
        ipstr = socket.inet_ntoa(struct.pack('!L', addr))
        # else:
        #     ipstr = addr

        if( ( addr_to_status[-1][addr] == 0) or (addr_to_status[0][addr]== 0) or (addr_to_status[1][addr] == 0) ):
            # addr_to_multiround_status[addr] = 0
            rda_multiround_op_fp.write("{0} 0\n".format(ipstr) )
        elif( ( addr_to_status[-1][addr] == 2) or (addr_to_status[0][addr] == 2) or (addr_to_status[1][addr] == 2) ):
            # addr_to_multiround_status[addr] = 2
            rda_multiround_op_fp.write("{0} 2\n".format(ipstr) )
        elif addr in resp_all_rounds_addr_set:
            # addr_to_multiround_status[addr] = 1
            rda_multiround_op_fp.write("{0} 1\n".format(ipstr) )
            
        # rda_multiround_op_fp.write("{0} {1}\n".format(ipstr, addr_to_multiround_status[addr]) )

    rda_multiround_op_fp.close() # wandio does not like it if the fp is not closed explicitly
            

this_t = int(sys.argv[1])
campaign = sys.argv[2]
read_bin = int(sys.argv[3]) # If read_bin == 1, we are reading binary input from resps_per_round.gz. Else we are reading ascii input from resps_per_addr.gz
is_swift = int(sys.argv[4]) # Whether we are reading input files from the Swift cluster or from disk

IS_COMPRESSED = 1
FAST_MODE = 1
num_adjacent_rounds = 1
ROUND_SECS = 600 # Perhaps define this in the zeusping_helpers header file
this_t_round_end = this_t + ROUND_SECS

if read_bin == 1:
    struct_fmt = struct.Struct("I 5H")
    buf = ctypes.create_string_buffer(struct_fmt.size * 2000)

# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'

if read_bin == 1:
    processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_testbin/'.format(campaign)
else:
    processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_testsimple/'.format(campaign)

setup_stuff(processed_op_dir, this_t, this_t_round_end)


unresp_addrs = {} # These are the set of unresponsive addresses at the beginning of a round (i.e., they were unresponsive in the previous round)
resp_addrs = {} # These are the set of responsive addresses at the beginning of a round (i.e., they were responsive in the previous round)
addr_to_status = {}

for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):

    # Get unresponsive and responsive addresses using the previous round.
    # responsive addresses are all the addresses that have the potential to dropout in this round.
    # unresponsive addresses are all the addresses that have the potential to anti-dropout in this round.

    if roun-1 not in unresp_addrs:
        unresp_addrs[roun-1] = set()
    if roun-1 not in resp_addrs:
        resp_addrs[roun-1] = set()

    get_resp_unresp(processed_op_dir, this_t + roun * ROUND_SECS, read_bin, is_swift, unresp_addrs[roun-1], resp_addrs[roun-1])

    # print len(unresp_addrs)

    # Get dropout and antidropout addresses using this round.
    # dropout addresses are all the addresses that responded last round but are unresponsive this round.
    # antidropout addresses are all the addresses that were unresponsive last round but responded this round.

    # dropout_addrs = set()
    # antidropout_addrs = set()

    if roun not in addr_to_status:
        addr_to_status[roun] = defaultdict(lambda:9999)

    if roun < num_adjacent_rounds:
        get_dropout_antidropout(processed_op_dir, this_t + roun * ROUND_SECS, read_bin, is_swift, unresp_addrs[roun-1], resp_addrs[roun-1], addr_to_status[roun])
    else:
        if roun not in resp_addrs:
            resp_addrs[roun] = set()
        # Get resp_addrs for this round too since it is the last round
        get_dropout_antidropout(processed_op_dir, this_t + roun * ROUND_SECS, read_bin, is_swift, unresp_addrs[roun-1], resp_addrs[roun-1], addr_to_status[roun], resp_addrs[roun])
        
# We're done with unresp_addrs, free memory
# del unresp_addrs
# gc.collect()

write_op(processed_op_dir, this_t, this_t_round_end, addr_to_status, resp_addrs)
