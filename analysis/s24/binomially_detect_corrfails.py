
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
import glob
import shlex
import subprocess
import os
import datetime
from collections import defaultdict
import array
import io
import wandio
import math
import numpy
import scipy
from scipy.stats import binom

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


def prep_calc_pd(fname, aggr_to_rd, pd_calc_tstart, pd_calc_tend):

    fp = open(fname)

    for line in fp:
        parts = line.strip().split('|')

        this_t = int(parts[0])

        if this_t < pd_calc_tstart or this_t >= pd_calc_tend:
            continue

        n_d = int(parts[2])
        n_r = int(parts[3])
        
        # TODO: If we want to calculate P(d) for every 24-h bin, we would need to init a dict for every 24h
        # We would also need to know what the 24h-bin this round corresponds to.
        
        aggr_to_rd["d"] += n_d        
        aggr_to_rd["r"] += n_r

    fp.close()
        

def obtain_pd(rd_dict):
    # Let us ensure statistical significance    
    sample_prop = rd_dict["d"]/float(rd_dict["r"])
    sample_prop_complement = 1 - sample_prop
    thresh = rd_dict["r"] * sample_prop * sample_prop_complement

    sys.stderr.write("Threshold is {0:.4f}\n".format(thresh) )

    if ( (thresh < 10) ):
        sys.stderr.write("Threshold is too small, not enough samples\n")
        sys.exit(1)

    sys.stderr.write("Probability of dropout is: {0:.6f}\n".format(sample_prop) )    
    p_d = sample_prop

    # This is the probability of observing two times as many dropout, or more accurately: the probability of observing a dropout over the next 2 rounds (which is what MR gives us). Doubling will deal with the fact that we will count each dropout twice when analyzing MR.
    # I considered another way to calculate p_d: where we would use total_n_d/total_n_r from the mr file. However, total_n_r in the mr file is not correct because we find n_r in the MR case differently. 
    p_d *= 2
    # NOTE: I performed some tests where I scaled p_d by 10X and even 920X. The minimum number of dropouts doesn't scale exactly by X, but is close enough. 

    sys.stderr.write("Probability of MR dropout is: {0}\n".format(p_d) )

    return p_d


def populate_sr_round_to_dets(sr_fname, round_to_dets, all_ts):
    fp = open(sr_fname)

    all_n_r = []
    line_ct = 0
    
    for line in fp:

        line_ct += 1
    
        parts = line.strip().split('|')

        this_t = int(parts[0])
        # We will get the number of responding addresses from the SR files
        n_r = int(parts[3])
        
        all_ts.append(this_t)
        round_to_dets[this_t] = {"n_r" : n_r}
        
        all_n_r.append(n_r)
        
    fp.close()

    all_n_r_np = numpy.array(all_n_r)
    med_n_r = numpy.median(all_n_r_np)
    
    return med_n_r

        
def find_min_outs_reqd(p_d, med_n_r, REQD_CONF):

    max_heuristic = float(med_n_r*0.65) # Even with a 0.5 P(f), no need to calculate beyond 0.65 * N        
    k = numpy.arange(max_heuristic)
    binomial_cdf = scipy.stats.binom.cdf(k, med_n_r, p_d)

    min_outs_reqd = 0
    for elem in binomial_cdf:
        # if elem > 0.9999:
        if elem > REQD_CONF:
        # if elem > 0.99:
        # if elem > 0.95:
            # print nups, nouts, min_outs_reqd, elem
            break
        min_outs_reqd += 1

    # At this point, the probability of min_outs_reqd or more failures is < REQD_CONF. Therefore, the minimum number of outages should be *greater* than min_outs_reqd, for there to be l.t. REQD_CONF probability that so many failures happened independently
    min_outs_reqd += 1

    return min_outs_reqd


def populate_mr_round_to_dets(mr_fname, round_to_dets, all_ts):

    fp = open(mr_fname)
    line_ct = 0
    for line in fp:

        line_ct += 1

        # if line_ct%100 == 0:
        #     sys.stderr.write("Processed {0} lines in MR at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )
            
        parts = line.strip().split('|')

        this_t = int(parts[0])

        n_d = int(parts[2])
        # n_r = int(parts[3])
        n_a = int(parts[4])
        
        # all_ts.append(this_t)
        round_to_dets[this_t]["n_d"] = n_d
        round_to_dets[this_t]["n_a"] = n_a

    fp.close()

    
def find_duration(round_to_dets, pot_time, pot_n_d):

    curr_t = pot_time + zeusping_helpers.ROUND_SECS
    
    while True:

        if curr_t not in round_to_dets:
            dur = -1
            return dur
        
        n_d = round_to_dets[curr_t]["n_d"]
        n_a = round_to_dets[curr_t]["n_a"]

        # if pot_time == 1611093600:
        #     sys.stderr.write("1611093600 {0} {1} {2}\n".format(pot_n_d, n_d, n_a) )

        # Anti-dropouts seem to so rarely occur in a correlated manner, so I am being lenient with matching. 
        if ( (n_a >= pot_n_d * 0.75) and (n_a <= pot_n_d * 1.2) ):
            dur = (curr_t - pot_time)/60 # In minutes
            if pot_time == 1611093600:
                sys.stderr.write("1611093600 {0} {1} {2} {3}\n".format(pot_n_d, n_d, n_a, dur) )

            return dur

        if n_a >= pot_n_d * 1.2:
            # A larger antidropout than what we can explain occurred. Return
            dur = -1
            return dur

        if n_d >= pot_n_d * 0.75:
             # Let's make sure this is not the round immediately after the correlated failure, since that round may have dropouts from this round.
            if curr_t != pot_time + zeusping_helpers.ROUND_SECS:
            
                # Another large dropout occurred before recovery. We will not know which of these dropouts a future anti-dropout will match
                dur = -1
                return dur

        # Let's ignore durations that are longer than 24 hours
        if curr_t > pot_time + 86400:
            dur = -1
            return dur
        
        curr_t += zeusping_helpers.ROUND_SECS


def populate_corrfails(round_to_dets, min_outs_reqd, all_ts, med_n_r, campaign, aggr, asn, rda_path):

    done_ts = set()
    
    for this_t in all_ts:

        if this_t in done_ts:
            continue

        done_ts.add(this_t)

        n_d = round_to_dets[this_t]["n_d"]
        n_a = round_to_dets[this_t]["n_a"]

        # A single outage cannot be a correlated failure!
        if n_d <= 1:
            continue

        n_r = round_to_dets[this_t]["n_r"]

        # if ( (n_r < 0.8 * med_n_r) or (n_r > 1.2 * med_n_r) ):
        if ( (n_r > 1.1 * med_n_r) ):            
            sys.stderr.write("Anomalous n_r, did not process: {0} {1} {2}\n".format(this_t, n_r, n_d) )
            continue
        
        if n_d > min_outs_reqd:

            # Let's check if one of the next rounds had even more dropouts than this one and add the round with the most dropouts
            pot_n_d = n_d
            pot_n_a = n_a
            pot_time = this_t

            next_t = this_t + zeusping_helpers.ROUND_SECS
            next_d = round_to_dets[next_t]["n_d"]
            next_a = round_to_dets[next_t]["n_a"]
            done_ts.add(next_t)

            if next_d > pot_n_d:
                pot_n_d = next_d
                pot_n_a = next_a
                pot_time = next_t

            # If an outage was spread across this round and the next round, the max outages would either be this round or next round (more likely next round). But it will not be in the next_next_round. So I decided to abandon the following logic.
            # next_next_t = next_t + zeusping_helpers.ROUND_SECS
            # next_next_d = round_to_dets[next_next_t]["n_d"]
            # next_next_a = round_to_dets[next_next_t]["n_a"]
            # done_ts.add(next_next_t)

            # if next_next_d > pot_n_d:
            #     pot_n_d = next_next_d
            #     pot_n_a = next_next_a
            #     pot_time = next_next_t

            # next_round's outages (if this outage had spread into it) will contribute to next_next_round. Instead of detecting an outage for next_next, let's skip it.
            # If next_next_round really had an outage, it would be observed in next_next_next_round.
            done_ts.add(next_t + zeusping_helpers.ROUND_SECS)

            # If there was a burst of anti-dropouts equal to the number of dropouts, ignore.
            if pot_n_a >= pot_n_d * 0.8:
                continue
            
            dur = find_duration(round_to_dets, pot_time, pot_n_d)
            sys.stderr.write("Found corrfail at {0} affecting {1} addresses with dur: {2}\n".format(pot_time, pot_n_d, dur) )

            # If it was a very short duration outage, return
            if dur == 10:
                continue

            if dur == -1:
                analyze_cmd = 'python analyze_s24s_per_as.py mr-oneround {0} {1} {2} {3} {4} "Duration:Unknown-pot_n_d:{5}"'.format(campaign, aggr, asn, rda_path, pot_time, pot_n_d)
                sys.stdout.write("{0}\n".format(analyze_cmd) )
                os.system(analyze_cmd)
            else:
                analyze_cmd = 'python analyze_s24s_per_as.py mr-oneround {0} {1} {2} {3} {4} "Duration:{5} minutes-pot_n_d:{6}"'.format(campaign, aggr, asn, rda_path, pot_time, dur, pot_n_d)
                sys.stdout.write("{0}\n".format(analyze_cmd) )
                os.system(analyze_cmd)
            
            # A correlated failure that is spread across 2 rounds (round T and T+1) can show up as affecting rounds T, T+1, and T+2.
            # Check among this round and the next 2 rounds which has the most dropouts
            
            # max_next_rounds = 2
            # while True:
            #     curr_t = this_t + zeusping_helpers.ROUND_SECS
            #     curr_d = round_to_dets[curr_t]["n_d"]
            #     done_ts.add(curr_t) # Whether the next round has more or fewer dropouts, we have already examined it, so should not examine it again.
            #     if curr_d <= pot_n_d:
            #         break

            #     if num_rounds >= max_next_rounds:
            #         break
                
            #     pot_n_d = curr_d
            #     pot_time = curr_t


def main():
    conf = 0.9999
    REQD_CONF = float(conf)

    campaign = sys.argv[1]
    aggr = sys.argv[2]
    asn = sys.argv[3]
    rda_path = sys.argv[4]
    pd_fname = sys.argv[5]
    pd_calc_tstart = int(sys.argv[6])
    pd_calc_tend = int(sys.argv[7])
    rda_sr_per_aggr_fname = sys.argv[8]
    rda_mr_per_aggr_fname = sys.argv[9]

    aggr_to_rd = defaultdict(int)
    # We will calculate P(D) using an arbitrary date range and we can apply Binomial test on a *different* date range than the one being examined. 
    prep_calc_pd(rda_sr_per_aggr_fname, aggr_to_rd, pd_calc_tstart, pd_calc_tend)
    p_d = obtain_pd(aggr_to_rd)
    # sys.exit(1)

    # The sr and mr files need to be opened consistently (i.e., they must have the same date range) for applying the Binomial test, so that we can calculate n_r using sr and n_d using mr.
    round_to_dets = {}
    all_ts = []
    med_n_r = populate_sr_round_to_dets(rda_sr_per_aggr_fname, round_to_dets, all_ts)
    min_outs_reqd = find_min_outs_reqd(p_d, med_n_r, REQD_CONF)
    # NOTE and TODO: I am using med_n_r to calculate min_outs_reqd as an efficiency measure.
    # Ideally, we would calculate min_outs_reqd for n_r in each round.
    # However, n_r is so stable that I decided to approximate it this way
    # If n_r << med_n_r in some round, it's Ok, our min_outs_reqd then is an overestimate. I take care of n_r >> med_n_r in populate_corrfails
    sys.stderr.write("Min outs reqd for {0} responses with p_d {1}: {2}\n".format(med_n_r, p_d, min_outs_reqd) )

    # If we just want to see P(D), min_outs_reqd etc., exit here.
    # sys.exit(1)
    
    min_outs_reqd = min_outs_reqd * 2 # NOTE: Temporary hack, to focus on larger outages. 
    populate_mr_round_to_dets(rda_mr_per_aggr_fname, round_to_dets, all_ts)

    # Detect all rounds where corrfails happen.
    # For each corrfail, we will then go through subsequent rounds. If a round has a large spike in anti-dropouts that matched the magnitude of the corrfail, we have a duration.
    # However, we can only go until we observe a second corrfail. After the second corrfail, we don't know if the spike in anti-dropouts is due to the first or the second.
    populate_corrfails(round_to_dets, min_outs_reqd, all_ts, med_n_r, campaign, aggr, asn, rda_path)


main()
