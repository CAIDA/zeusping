
#  This software is Copyright (c) 2020 The Regents of the University of
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
import random
import math
from collections import defaultdict

op_fname_pref = sys.argv[1]
num_samples = int(sys.argv[2])
# op_fp = open(op_fname, 'w')

asn_to_addrs = {}
asn_to_tot_addrs = defaultdict(int)

line_ct = 0
tot_addrs = 0

for line in sys.stdin:

    line_ct += 1

    if line_ct % 100000 == 0:
        sys.stderr.write("Lines processed: {0}\n".format(line_ct) )
    
    parts = line.strip().split('|')

    if (len(parts) != 2):
        continue
    
    addr = parts[0].strip()
    asn = parts[1].strip()

    if asn not in asn_to_addrs:
        asn_to_addrs[asn] = []
    asn_to_addrs[asn].append(addr)

    asn_to_tot_addrs[asn] += 1

    tot_addrs += 1


top_20_tot_addrs = 0
sorted_tuple = sorted(asn_to_tot_addrs.items(), key=lambda kv: kv[1], reverse=True)


top_20_asns = set()
num_asns_done = 0
for entry in sorted_tuple:

    if num_asns_done == 20:
        break

    asn = entry[0]

    top_20_asns.add(asn)
    
    num_addrs = entry[1]

    # print asn, num_addrs
    
    top_20_tot_addrs += num_addrs

    num_asns_done += 1

print top_20_tot_addrs


# def split_ips(ip):
#     parts = ip.strip().split('.')
#     oct1 = parts[0].strip()
#     oct2 = parts[1].strip()
#     oct3 = parts[2].strip()
#     oct4 = parts[3].strip()

#     return oct1, oct2, oct3, oct4


# def find_s24(ipv4_addr):
#     oct1, oct2, oct3, oct4 = split_ips(ipv4_addr)
#     return "{0}.{1}.{2}.0/24".format(oct1, oct2, oct3)

op_fps = {}
for file_idx in range(num_samples):
    op_fps[file_idx] = open('./data/{0}_{1}samples_s{2}_unsorted'.format(op_fname_pref, num_samples, file_idx+1), 'w')
    
    
for asn in asn_to_addrs:

    num_addrs_asn = asn_to_tot_addrs[asn]

    # num_addrs_asn_per_file = math.ceil(num_addrs_asn/float(num_samples)) # If we have 9 addresses in an AS and 4 files, we will obtain ceil(9/4) = 3 addrs per file. So first 3 addrs go into file 1, next 3 addrs go into file 2, next 3 go into file 3 and nothing into file 4!    
    # num_addrs_asn_per_file = num_addrs_asn/float(num_samples) # This didn't solve the above problem; in fact the outputs were identical (of course)!

    # this_asn_share = 1000 * num_addrs_asn/float(top_20_tot_addrs)
    this_asn_addrs = asn_to_addrs[asn]
    random.shuffle(this_asn_addrs)

    # file_idx = 0
    written_ct = 0

    for addr in this_asn_addrs:

        # if written_ct >= num_addrs_asn_per_file:
        #     file_idx += 1
        #     written_ct = 0

        file_idx = written_ct%num_samples

        op_fps[file_idx].write("{0}|{1}\n".format(addr, asn) )
        written_ct += 1
    
