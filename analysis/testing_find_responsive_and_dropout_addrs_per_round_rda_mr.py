
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

        addr = parts[0]
        addr_status = parts[1]

        if addr_status not in status:
            status[addr_status] = set()
        
        status[addr_status].add(addr)

    return status

    
def test_simple_round_rda_mr(round_tstamp):
    prev_prev_round_tstamp = round_tstamp - (2 * zeusping_helpers.ROUND_SECS)

    prev_prev_round_rda_simple = "{0}/responsive_and_dropout_addrs/{1}_to_{2}.gz".format(old_path, prev_prev_round_tstamp, prev_prev_round_tstamp + zeusping_helpers.ROUND_SECS)
    prev_prev_status = find_rda(prev_prev_round_rda_simple)

    prev_round_tstamp = round_tstamp - zeusping_helpers.ROUND_SECS
    prev_round_rda_simple = "{0}/responsive_and_dropout_addrs/{1}_to_{2}.gz".format(old_path, prev_round_tstamp, prev_round_tstamp + zeusping_helpers.ROUND_SECS)
    prev_status = find_rda(prev_round_rda_simple)

    this_round_rda_simple = "{0}/responsive_and_dropout_addrs/{1}_to_{2}.gz".format(old_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    this_status = find_rda(this_round_rda_simple)
    
    next_round_tstamp = round_tstamp + zeusping_helpers.ROUND_SECS
    next_round_rda_simple = "{0}/responsive_and_dropout_addrs/{1}_to_{2}.gz".format(old_path, next_round_tstamp, next_round_tstamp + zeusping_helpers.ROUND_SECS)
    next_status = find_rda(next_round_rda_simple)

    next_next_round_tstamp = round_tstamp + (2 * zeusping_helpers.ROUND_SECS)
    next_next_round_rda_simple = "{0}/responsive_and_dropout_addrs/{1}_to_{2}.gz".format(old_path, next_next_round_tstamp, next_next_round_tstamp + zeusping_helpers.ROUND_SECS)
    next_next_status = find_rda(next_next_round_rda_simple)
    
    rda_mr_fname = "{0}/{1}_to_{2}/rda_multiround_test.gz".format(new_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    rda_status = find_rda(rda_mr_fname)

    # Addresses that experienced dropouts in prev_round should have been recorded as experiencing dropouts in rda_status
    # for addr in prev_status['0']:
    #     if addr in rda_status['0']:
    #         sys.stdout.write("Good. {0} dropout in prev and in rda_mr\n".format(addr) )

    if prev_status['0'].issubset(rda_status['0']):
        sys.stdout.write("Good. Addresses that dropped out in prev_round are a subset of dropout addresses this multiround {0}\n".format(round_tstamp) )
    else:
        sys.stdout.write("Bad. Addresses that dropped out in prev_round are not a subset of dropout addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)

    # Addresses that experienced antidropouts in prev_round should have been recorded as experiencing antidropouts in rda_status
    # for addr in prev_status['2']:
    #     if addr in rda_status['2']:
    #         sys.stdout.write("Good. {0} antidropout in prev and in rda_mr\n".format(addr) )

    if prev_status['2'].issubset(rda_status['2']):
        sys.stdout.write("Good. Addresses that antidropped out in prev_round are a subset of antidropout addresses this multiround {0}\n".format(round_tstamp) )
    else:
        sys.stdout.write("Bad. Addresses that antidropped out in prev_round are not a subset of antidropout addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)
    
    # Addresses that experienced dropouts in this_round should have been recorded as experiencing dropouts in rda_status    
    # for addr in this_status['0']:
    #     if addr in rda_status['0']:
    #         sys.stdout.write("Good. {0} dropout in this and in rda_mr\n".format(addr) )

    if this_status['0'].issubset(rda_status['0']):
        sys.stdout.write("Good. Addresses that dropped out in this_round are a subset of dropout addresses this multiround {0}\n".format(round_tstamp) )
    else:
        sys.stdout.write("Bad. Addresses that dropped out in this_round are not a subset of dropout addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)
    
    # Addresses that experienced antidropouts in this_round should have been recorded as experiencing antidropouts in rda_status    
    # for addr in this_status['2']:
    #     if addr in rda_status['2']:
    #         sys.stdout.write("Good. {0} antidropout in this and in rda_mr\n".format(addr) )

    if this_status['2'].issubset(rda_status['2']):
        sys.stdout.write("Good. Addresses that antidropped out in this_round are a subset of antidropout addresses this multiround {0}\n".format(round_tstamp) )
    else:
        sys.stdout.write("Bad. Addresses that antidropped out in this_round are not a subset of antidropout addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)
    
    # Addresses that experienced dropouts in next_round should *not* have been recorded as experiencing dropouts in rda_status (unless they dropped out in prev_round as well)
    # for addr in next_status['0']:
    #     if addr not in prev_status['0']:
    #         if addr in rda_status['0']:
    #             sys.stdout.write("Bad. {0} dropout in this and in rda_mr\n".format(addr) )

    if (next_status['0'] - prev_status['0']).issubset(rda_status['0']):
        sys.stdout.write("Bad. Addresses that dropped out in next_round are a subset of dropout addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)
    else:
        sys.stdout.write("Good. Addresses that dropped out in next_round are not a subset of dropout addresses this multiround {0}\n".format(round_tstamp) )
        
    # Addresses that experienced antidropouts in next_round should *not* have been recorded as experiencing antidropouts in rda_status (unless they anti-dropped out in prev_round as well)
    # for addr in next_status['2']:
    #     if addr not in prev_status['2']:
    #         if addr in rda_status['2']:
    #             sys.stdout.write("Bad. {0} antidropout in this and in rda_mr\n".format(addr) )

    if (next_status['2'] - prev_status['2']).issubset(rda_status['2']):
        sys.stdout.write("Bad. Addresses that antidropped out in next_round are a subset of antidropout addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)
    else:
        sys.stdout.write("Good. Addresses that antidropped out in next_round are not a subset of antidropout addresses this multiround {0}\n".format(round_tstamp) )
        
    # Even addresses that were responsive this round but dropped out in an earlier round (like prev_prev) should not have been recorded as responsive
    prev_prev_d_this_r = this_status['1'] & prev_prev_status['0']
    sys.stderr.write("{0} addresses dropped out in prev_prev but are responsive now\n".format(len(prev_prev_d_this_r) ) )
    if prev_prev_d_this_r.issubset(rda_status['1']):
        sys.stdout.write("Bad. Addresses that dropped out in prev_prev but responded this round are a subset of responsive addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)
    else:
        sys.stdout.write("Good. Addresses that dropped out in prev_prev but responded this round are not a subset of responsive addresses this multiround {0}\n".format(round_tstamp) )        
    
    # Addresses that experienced dropouts in any of prev_prev, prev, this, next should *not* have been recorded as responsive in rda_status
    dropouts_union = prev_prev_status['0'] | prev_status['0'] | this_status['0'] | next_status['0']
    # for addr in dropouts_union:
    #     if addr in rda_status['1']:
    #         sys.stdout.write("Bad. {0} in rda_mr but should not be\n".format(addr) )
    #         sys.exit(1)

    if dropouts_union.issubset(rda_status['1']):
        sys.stdout.write("Bad. Addresses that dropped out in one of prev_prev, prev, this, next are a subset of responsive addresses this multiround {0}\n".format(round_tstamp) )
        sys.exit(1)
    else:
        sys.stdout.write("Good. Addresses that dropped out in one of prev_prev, prev, this, next are not a subset of responsive addresses this multiround {0}\n".format(round_tstamp) )
    
    resp_intersection = prev_status['1'] & this_status['1'] & next_status['1'] & next_next_status['1']
    # sys.stdout.write("resp_intersection len: {0}\n".format(len(resp_intersection) ) )
    # sys.stdout.write("rda_mr resp len: {0}\n".format(len(rda_status['1']) ) )    
    if resp_intersection == rda_status['1']:
        sys.stdout.write("Good. resp addresses in rda_mr are correct {0}\n".format(round_tstamp) )
    else:
        sys.stdout.write("Bad. resp addresses in rda_mr are incorrect {0}\n".format(round_tstamp) )
        sys.exit(1)

    sys.stdout.flush()


def test_mrptn_round_rda_mr(round_tstamp):
    old_rda_mr_fname = "{0}/{1}_to_{2}/rda_multiround_test.gz".format(old_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    old_rda_status = find_rda(old_rda_mr_fname)

    rda_mr_fname = "{0}/{1}_to_{2}/rda_multiround_test.gz".format(new_path, round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    rda_status = find_rda(rda_mr_fname)

    prev_prev_round_tstamp = round_tstamp - (2*zeusping_helpers.ROUND_SECS)
    prev_prev_round_rda_sr_fname = "{0}/{1}_to_{2}/rda_test.gz".format(new_path, prev_prev_round_tstamp, prev_prev_round_tstamp + zeusping_helpers.ROUND_SECS)
    prev_prev_status = find_rda(prev_prev_round_rda_sr_fname)
    
    prev_round_tstamp = round_tstamp - zeusping_helpers.ROUND_SECS
    prev_round_rda_sr_fname = "{0}/{1}_to_{2}/rda_test.gz".format(new_path, prev_round_tstamp, prev_round_tstamp + zeusping_helpers.ROUND_SECS)
    prev_status = find_rda(prev_round_rda_sr_fname)
    
    next_round_tstamp = round_tstamp + zeusping_helpers.ROUND_SECS
    next_round_rda_sr_fname = "{0}/{1}_to_{2}/rda_test.gz".format(new_path, next_round_tstamp, next_round_tstamp + zeusping_helpers.ROUND_SECS)
    next_status = find_rda(next_round_rda_sr_fname)

    # Check dropouts
    if len(old_rda_status['0']) > len(rda_status['0']):
        sys.stdout.write("Good. old_rda has {0} mr dropouts while rda has {1} mr dropouts in round {2}\n".format(len(old_rda_status['0']), len(rda_status['0']), round_tstamp ) )

        # old_rda should have dropouts that include dropouts from next_round. Subtract them. But add back addresses that had dropouts in *both* next_round and prev_round.
        correct_dropouts = (old_rda_status['0'] - next_status['0']) | (prev_status['0'] & next_status['0'])
        if correct_dropouts == rda_status['0']:
            sys.stdout.write("Good. Correct calculation of dropouts in round {0}\n".format(round_tstamp ) )
        else:
            sys.stdout.write("Bad. {0} correct_dropouts, {1} rda_mr dropouts; incorrect calculation of dropouts in round {2}\n".format(len(correct_dropouts), len(rda_status['0']), round_tstamp ) )
            sys.exit(1)
    else:
        sys.stdout.write("Bad. old_rda has {0} mr dropouts while rda has {1} mr dropouts in round {2}\n".format(len(old_rda_status['0']), len(rda_status['0']), round_tstamp ) )
        sys.exit(1)

    # Check antidropouts        
    if len(old_rda_status['2']) > len(rda_status['2']):
        sys.stdout.write("Good. old_rda has {0} mr antidropouts while rda has {1} mr antidropouts in round {2}\n".format(len(old_rda_status['2']), len(rda_status['2']), round_tstamp ) )

        # old_rda should have antidropouts that include antidropouts from next_round. Subtract them. But add back addresses that had antidropouts in *both* next_round and prev_round.
        correct_antidropouts = (old_rda_status['2'] - next_status['2']) | (prev_status['2'] & next_status['2'])
        if correct_antidropouts == rda_status['2']:
            sys.stdout.write("Good. Correct calculation of antidropouts in round {0}\n".format(round_tstamp ) )
        else:
            sys.stdout.write("Bad. {0} correct_antidropouts, {1} rda_mr antidropouts; incorrect calculation of antidropouts in round {2}\n".format(len(correct_antidropouts), len(rda_status['2']), round_tstamp ) )
            sys.exit(1)
    else:
        sys.stdout.write("Bad. old_rda has {0} mr antidropouts while rda has {1} mr antidropouts in round {2}\n".format(len(old_rda_status['2']), len(rda_status['2']), round_tstamp ) )
        sys.exit(1)

    # Check responsive
    if len(old_rda_status['1']) > len(rda_status['1']):
        sys.stdout.write("Good. old_rda has {0} mr resps while rda has {1} mr resps in round {2}\n".format(len(old_rda_status['1']), len(rda_status['1']), round_tstamp ) )

        correct_resps = old_rda_status['1'] & prev_status['1']
        if correct_resps == rda_status['1']:
            sys.stdout.write("Good. Correct calculation of resps in round {0}\n".format(round_tstamp ) )
        else:
            sys.stdout.write("Bad. {0} correct_resps, {1} rda_mr resps; incorrect calculation of resps in round {2}\n".format(len(correct_resps), len(rda_status['1']), round_tstamp ) )
            sys.exit(1)

        # Most rem_addrs are addresses that had been out during prev_prev but experienced an anti-dropout during prev            
        # rem_addrs = ( (old_rda_status['1'] - rda_status['1']) - prev_prev_status['0'])
        # sys.stderr.write("{0} remaining addresses\n".format(len(rem_addrs)) )
        # for addr in rem_addrs:
        #     sys.stderr.write("{0}\n".format(addr) )

        # After subtracting anti-dropouts during prev round, we are left with a handful of addresses that had ICMP error related weirdness
        # rem_addrs = ( ( (old_rda_status['1'] - rda_status['1']) - prev_prev_status['0']) - prev_status['2'])
        # sys.stderr.write("{0} remaining addresses\n".format(len(rem_addrs)) )
        # for addr in rem_addrs:
        #     sys.stderr.write("{0}\n".format(addr) )
        
    else:
        sys.stdout.write("Bad. old_rda has {0} mr resps while rda has {1} mr resps in round {2}\n".format(len(old_rda_status['1']), len(rda_status['1']), round_tstamp ) )
        sys.exit(1)
    
    
test_mode = sys.argv[1] # simple to test against old method, mrptn to test against method where we considered dropouts and anti-dropouts across prev, this, next    
start_round_epoch = int(sys.argv[2])
end_round_epoch = int(sys.argv[3])


if test_mode == "simple":
    old_path = "/scratch/zeusping/data/processed_op_CA_ME_testsimple"
else:
    old_path = "/scratch/zeusping/data/processed_op_CA_ME_testbintest2"
new_path = "/scratch/zeusping/data/processed_op_CA_ME_testbintest3"


for round_tstamp in range(start_round_epoch, end_round_epoch, zeusping_helpers.ROUND_SECS):
    sys.stderr.write("Processing round {0}\n".format(round_tstamp) )

    if test_mode == "simple":
        test_simple_round_rda_mr(round_tstamp)
    else:
        test_mrptn_round_rda_mr(round_tstamp)
    
