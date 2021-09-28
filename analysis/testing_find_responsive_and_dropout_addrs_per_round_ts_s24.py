
#  This software is Copyright (c) 2021 The Regents of the University of
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
import os
import subprocess
import shlex
import wandio
import pprint
import struct
import socket
from collections import defaultdict

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

if sys.version_info[0] == 2:
    py_ver = 2
else:
    py_ver = 3


def find_rda(fname):
    sys.stderr.write("Processing rda fname {0}\n".format(fname) )
    fp = wandio.open(fname)

    status = {}
    
    for line in fp:
        parts = line.strip().split()

        ipstr = parts[0]
        ipid = struct.unpack("!I", socket.inet_aton(ipstr))[0]

        addr_status = parts[1]

        if addr_status not in status:
            status[addr_status] = set()
        
        status[addr_status].add(ipid)

    return status


def find_s24_status_from_rda(this_status, s24_to_status_simple):
    for addr_status in this_status:
        for ipid in this_status[addr_status]:
            s24 = ipid & s24_mask
            oct4 = ipid & oct4_mask

            if s24 not in s24_to_status_simple:
                s24_to_status_simple[s24] = {"0" : set(), "1" : set(), "2" : set()}

            s24_to_status_simple[s24][addr_status].add(oct4)
            

def find_s24_status_new(fname, s24_to_status_new):
    sys.stderr.write("Processing ts_s24_fname {0}\n".format(fname) )
    fp = wandio.open(fname)

    for line in fp:
        parts = line.strip().split('|')
        s24 = int(parts[0].strip() )

        s24_to_status_new[s24] = defaultdict(set)
        
        d_addrs = int(parts[1])
        zeusping_helpers.find_addrs_in_s24_with_status(s24, d_addrs, '0', s24_to_status_new[s24], is_s24_str=False)

        r_addrs = int(parts[2])
        zeusping_helpers.find_addrs_in_s24_with_status(s24, r_addrs, '1', s24_to_status_new[s24], is_s24_str=False)
        
        a_addrs = int(parts[3])
        zeusping_helpers.find_addrs_in_s24_with_status(s24, a_addrs, '2', s24_to_status_new[s24], is_s24_str=False)
        
    
def test_round_ts_s24(round_tstamp):

    if mode == "sr":
        this_round_rda_simple = "{0}/responsive_and_dropout_addrs/{1}_to_{2}.gz".format(simple_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
        this_status_rda = find_rda(this_round_rda_simple)
    else:
        this_round_rda_mr = "{0}/{1}_to_{2}/rda_multiround_test.gz".format(new_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
        this_status_rda = find_rda(this_round_rda_mr)

    s24_to_status_rda = {}
    find_s24_status_from_rda(this_status_rda, s24_to_status_rda)

    s24_to_status_new = {}
    if mode == "sr":
        ts_s24_fname = "{0}/{1}_to_{2}/ts_s24_sr_test.gz".format(new_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    else:
        ts_s24_fname = "{0}/{1}_to_{2}/ts_s24_mr_test.gz".format(new_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    find_s24_status_new(ts_s24_fname, s24_to_status_new)

    if len(s24_to_status_new) != len(s24_to_status_rda):
        sys.stdout.write("Bad. Number of s24s is different\n".format(s24) )
        sys.exit(1)
    else:
        sys.stdout.write("Good. Number of s24s is identical in round {0}\n".format(round_tstamp) )
    
    for s24 in s24_to_status_new:
        if s24 not in s24_to_status_rda:
            sys.stdout.write("Bad. Missing s24: {0}\n".format(s24) )
            sys.exit(1)
        else:
            
    #         for oct4 in s24_to_status_rda[s24]['0']:
    #             if oct4 in s24_to_status_new[s24]['0']:
    #                 s24_str = socket.inet_ntoa(struct.pack('!L', s24))
    #                 # sys.stderr.write("{0} {1}\n".format(s24_str, s24) )
    #                 # sys.exit(1)
    #                 s24_pref = s24_str[:-1]
    #                 sys.stdout.write("Good. Addr in both: {0}{1}\n".format(s24_pref, oct4) )
    #                 # sys.exit(1)

    #         # for oct4 in s24_to_status_rda[s24]['1']:
    #         #     if oct4 in s24_to_status_new[s24]['1']:
    #         #         s24_str = socket.inet_ntoa(struct.pack('!L', s24))
    #         #         s24_pref = s24_str[:-1]
    #         #         sys.stdout.write("Good. Addr in both: {0}{1}\n".format(s24_pref, oct4) )
                
    #         # for oct4 in s24_to_status_rda[s24]['2']:
    #         #     if oct4 in s24_to_status_new[s24]['2']:
    #         #         s24_str = socket.inet_ntoa(struct.pack('!L', s24))
    #         #         s24_pref = s24_str[:-1]
    #         #         sys.stdout.write("Good. Addr in both: {0}{1}\n".format(s24_pref, oct4) )

            if ( (s24_to_status_rda[s24]['0'] == s24_to_status_new[s24]['0']) and
                 (s24_to_status_rda[s24]['1'] == s24_to_status_new[s24]['1']) and
                 (s24_to_status_rda[s24]['2'] == s24_to_status_new[s24]['2'])
            ):
                sys.stdout.write("Good. s24 {0} in Round {1} confirmed\n".format(s24, round_tstamp) )
            else:
                sys.stdout.write("Bad. s24 {0} in Round {1} failed\n".format(s24, round_tstamp) )
                sys.exit(1)
                 
            
mode = sys.argv[1] # mode == sr for testing sr output and mr for testing mr output
start_round_epoch = int(sys.argv[2])
end_round_epoch = int(sys.argv[3])

# These are rounds with known anomalies that we want to skip the diff test
iggy_rounds = set()
iggy_rounds.add(1617497400)
iggy_rounds.add(1617561600)

simple_path = "/scratch/zeusping/data/processed_op_CA_ME_testsimple"
new_path = "/scratch/zeusping/data/processed_op_CA_ME_testbintest3"

s24_mask = 0
for i in range(24):
    s24_mask |= 1 << i
s24_mask = s24_mask << 8

oct4_mask = (1 << 8) - 1

for round_tstamp in range(start_round_epoch, end_round_epoch, zeusping_helpers.ROUND_SECS):

    if round_tstamp in iggy_rounds:
        continue
    
    sys.stderr.write("Processing round {0}\n".format(round_tstamp) )
    test_round_ts_s24(round_tstamp)
    
