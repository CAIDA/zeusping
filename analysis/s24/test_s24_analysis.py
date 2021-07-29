

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

# pinged_ips_fname is a file that contains all the addresses that were pinged in this AS. This file is essential since we use it to prune the set of addresses in the rda file that we will wade through.
# resp_s24s_fname is a file that contains: for each /24, the addresses that were *typically* responsive in this AS during a particular window of time that we consider as being representative. 
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
import pprint

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


# Specify
old_method_fname = sys.argv[1]
new_method_fname = sys.argv[2]

def populate_old_s24_to_dets(old_method_fname):

    s24_to_dets = {}
    sys.stderr.write("Opening {0}\n".format(old_method_fname) )
    fp = open(old_method_fname)
    for line in fp:
        parts = line.strip().split('|')
        s24 = parts[0].strip()
        n_d = int(parts[1].strip() )
        n_r = int(parts[2].strip() )
        n_a = int(parts[3].strip() )

        if s24 not in s24_to_dets:
            s24_to_dets[s24] = {}
            
        s24_to_dets[s24]['d'] = n_d
        s24_to_dets[s24]['r'] = n_r
        s24_to_dets[s24]['a'] = n_a
        
    fp.close()
    return s24_to_dets


old_s24_to_dets = populate_old_s24_to_dets(old_method_fname)

fp = open(new_method_fname)
for line in fp:
    parts = line.strip().split('|')
    
    s24 = parts[0].strip()

    # pprint.pprint(s24)
    # sys.exit(1)

    if s24 not in old_s24_to_dets:
        continue
    else:
        this_old_val = old_s24_to_dets[s24]

    s24_to_dets = defaultdict(set)
    
    d_addrs = int(parts[1])
    zeusping_helpers.find_addrs_in_s24_with_status(s24, d_addrs, 'd', s24_to_dets)

    r_addrs = int(parts[2])
    zeusping_helpers.find_addrs_in_s24_with_status(s24, r_addrs, 'r', s24_to_dets)

    a_addrs = int(parts[3])
    zeusping_helpers.find_addrs_in_s24_with_status(s24, a_addrs, 'a', s24_to_dets)
    
    if ( (len(s24_to_dets['d']) == this_old_val['d']) and (len(s24_to_dets['r']) == this_old_val['r']) and (len(s24_to_dets['a']) == this_old_val['a'] ) ):
        sys.stderr.write("Matched! ")
        sys.stderr.write("s24: {0}, n_d_new: {1}, n_r_new: {2}, n_a_new: {3}, n_d_old: {4}, n_r_old: {5}, n_a_old: {6}\n".format(s24, len(s24_to_dets['d']), len(s24_to_dets['r']), len(s24_to_dets['a']), this_old_val['d'], this_old_val['r'], this_old_val['a'] ) )
    else:
        sys.stderr.write("Not matched! ")
        sys.stderr.write("s24: {0}, n_d_new: {1}, n_r_new: {2}, n_a_new: {3}, n_d_old: {4}, n_r_old: {5}, n_a_old: {6}\n".format(s24, len(s24_to_dets['d']), len(s24_to_dets['r']), len(s24_to_dets['a']), this_old_val['d'], this_old_val['r'], this_old_val['a'] ) )
        
        # sys.exit(1)
        
