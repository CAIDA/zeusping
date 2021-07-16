
# pinged_ips_fname is a file that contains all the addresses that were pinged in this AS. This file is essential since we use it to prune the set of addresses in the rda file that we will wade through.
# resp_ips_fname is a file that contains the addresses that were *typically* responsive in this AS. I got it by setting some arbitrary thresholds (for e.g.: addresses that responded more than X times when they had been pinged Y times, using addr_to_dropouts_detailed). This file is not strictly essential for the /24-based analysis; if we use it, it gives us a sense of how many addresses in the /24 are likely to be ping-responsive
# specific_round_fname is the rda (responsive, dropouts, anti-dropouts) file for the specific round we're interested in.

import sys
import glob
import shlex
import subprocess
import os
import datetime
import json
from collections import defaultdict
import array
import io
import struct
import socket
import wandio

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

# We'd like to find how many addresses we pinged in each /24
# How many of those addresses responded to pings in general
# How many addresses responded to pings from the /24s that Marder cared about

def split_ips(ip):
    parts = ip.strip().split('.')
    # print parts
    oct1 = parts[0].strip()
    oct2 = parts[1].strip()
    oct3 = parts[2].strip()
    oct4 = parts[3].strip()

    return oct1, oct2, oct3, oct4


def find_s24(ipv4_addr):
    oct1, oct2, oct3, oct4 = split_ips(ipv4_addr)
    return "{0}.{1}.{2}.0/24".format(oct1, oct2, oct3)


def populate_s24_to_dets(fname, typ):
    fp = open(fname)

    if typ == "pinged":
        pinged_addrs = set()

    for line in fp:

        parts = line.strip().split('|')
        addr = parts[0].strip()

        if typ == "pinged":
            pinged_addrs.add(addr)

        s24 = find_s24(addr)

        if s24 not in s24_to_dets:
            # s24_to_dets[s24] = {"pinged" : set(), "resp" : set()}
            # if mode == "simple":
            #     s24_to_dets[s24] = {"pinged" : set(), "resp" : set(), "d" : set(), "r" : set(), "a" : set()}
            # else:
            # I considered getting rid of resp() altogether but decided to keep it after all. It may be of use some day...
            # if mode == "simple-oneround" or mode == "mr-oneround":
            #     s24_to_dets[s24] = {"pinged" : set(), "resp" : set(), "d" : set(), "r" : set(), "a" : set()}
            # else:
            # s24_to_dets can take on keys "pinged", "resp" for all modes
            # s24_to_dets can take on keys "d", "r", "a" for singleround modes. For multiround modes, we will use s24_to_rda_dets for "d", "r", "a"
            s24_to_dets[s24] = defaultdict(set)

        s24_to_dets[s24][typ].add(addr)

    if typ == "pinged":
        return pinged_addrs


def find_reqd_s24_set(pinged_addrs):
    reqd_s24_set = set()

    for addr in pinged_addrs:
        s24 = find_s24(addr)

        reqd_s24_set.add(s24)

    return reqd_s24_set
    
    
def populate_s24_to_round_status(fname, reqd_set):

    if 'gz' in fname:
        fp = wandio.open(fname)
    else:
        fp = open(fname)

    for line in fp:

        parts = line.strip().split()
        
        addr = parts[0].strip()

        if addr not in reqd_set:
            continue
        
        status = parts[1].strip()

        s24 = find_s24(addr)
        this_k = status_to_char[status] # this_k will be 'd', 'r', or 'a'
        
        # NOTE: The following is not necessary since s24_to_dets = defaultdict(set)
        # if this_k not in s24_to_dets[s24]:
        #     s24_to_dets[s24][this_k] = set()

        s24_to_dets[s24][this_k].add(addr)

        
def populate_s24_to_round_status_mr(fname, reqd_s24_set, s24_to_dets):

    if 'gz' in fname:
        fp = wandio.open(fname)
    else:
        fp = open(fname)

    for line in fp:

        parts = line.strip().split('|')

        s24 = parts[0].strip()

        if s24 not in reqd_s24_set:
            continue

        d_addrs = int(parts[1])
        # NOTE: The following is not necessary since s24_to_dets = defaultdict(set)
        # if 'd' not in s24_to_dets[s24]:
        #     s24_to_dets[s24]['d'] = set()
        zeusping_helpers.find_addrs_in_s24_with_status(s24, d_addrs, 'd', s24_to_dets[s24])
        
        r_addrs = int(parts[2])
        # NOTE: The following is not necessary since s24_to_dets = defaultdict(set)
        # if 'r' not in s24_to_dets[s24]:
        #     s24_to_dets[s24]['r'] = set()
        zeusping_helpers.find_addrs_in_s24_with_status(s24, r_addrs, 'r', s24_to_dets[s24])

        a_addrs = int(parts[3])
        # NOTE: The following is not necessary since s24_to_dets = defaultdict(set)
        # if 'a' not in s24_to_dets[s24]:
        #     s24_to_dets[s24]['a'] = set()
        zeusping_helpers.find_addrs_in_s24_with_status(s24, a_addrs, 'a', s24_to_dets[s24])


# simple-oneround for the mode where we use the output of swift_process_round_simple for single round.
# mr-oneround for the mode where we use the status of /24s calculated across multiple rounds, but only look at a given round
# mr-multiround where we take in a start-time and end-time, and identify all rounds where more than threshold dropouts occurred with very few responsive addresses. For those rounds, write into an output file. We will dig into those rounds individually later.
mode = sys.argv[1]         
pinged_ips_fname = sys.argv[2]
resp_ips_fname = sys.argv[3]


status_to_char = {"0" : "d", "1" : "r", "2" : "a"}
s24_to_dets = {}

pinged_addrs = populate_s24_to_dets(pinged_ips_fname, "pinged")
populate_s24_to_dets(resp_ips_fname, "resp") # Note: I am using "resp" to identify how many addresses "typically" respond to pings from each /24.

# print s24_to_dets['24.30.242.0/24']

if mode == "simple-oneround":
    specific_round_fname = sys.argv[4]
    populate_s24_to_round_status(specific_round_fname, pinged_addrs)
    for s24 in s24_to_dets:
        # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_dets[s24]["resp"]), len(s24_to_dets[s24]["d"]),  len(s24_to_dets[s24]["r"]),  len(s24_to_dets[s24]["a"]) ) )
        sys.stdout.write("{0} {1} {2} {3}\n".format(s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_dets[s24]["d"]),  len(s24_to_dets[s24]["r"]) ) )
    

elif mode == "mr-oneround":
    specific_round_fname = sys.argv[4]
    reqd_s24_set = find_reqd_s24_set(pinged_addrs)
    populate_s24_to_round_status_mr(specific_round_fname, reqd_s24_set, s24_to_dets)
    for s24 in s24_to_dets:
        # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_dets[s24]["resp"]), len(s24_to_dets[s24]["d"]),  len(s24_to_dets[s24]["r"]),  len(s24_to_dets[s24]["a"]) ) )
        sys.stdout.write("{0} {1} {2} {3}\n".format(s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_dets[s24]["d"]),  len(s24_to_dets[s24]["r"]) ) )

elif mode == "mr-multiround":
    inp_path = sys.argv[4]
    tstart = int(sys.argv[5])
    tend = int(sys.argv[6])

    def nested_dict_factory_set(): 
        return defaultdict(set)
    
    reqd_s24_set = find_reqd_s24_set(pinged_addrs)
    for this_t in range(tstart, tend, zeusping_helpers.ROUND_SECS):
        s24_to_rda_dets = defaultdict(nested_dict_factory_set)
        this_t_fname = "{0}/{1}_to_{2}/ts_s24_mr_test".format(inp_path, this_t, this_t + zeusping_helpers.ROUND_SECS)
        populate_s24_to_round_status_mr(this_t_fname, reqd_s24_set, s24_to_rda_dets)

        sys.stderr.write("{0} processed, {1} s24s obtained\n".format(this_t, len(s24_to_rda_dets) ) )
        
        for s24 in s24_to_rda_dets:
            if len(s24_to_rda_dets[s24]['d']) >= 5 and len(s24_to_rda_dets[s24]['r']) < 5:
                sys.stdout.write("{0} {1} {2} {3} {4} {5} {6}\n".format(this_t, str(datetime.datetime.utcfromtimestamp(this_t)), s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_rda_dets[s24]["d"]), len(s24_to_rda_dets[s24]["r"]), len(s24_to_rda_dets[s24]["a"]) ) )

        sys.stdout.flush()

