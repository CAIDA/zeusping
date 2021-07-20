
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
from collections import defaultdict

op_fname = sys.argv[1]


op_fp = open(op_fname, 'w')

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


for asn in asn_to_addrs:

    if asn in top_20_asns:

        num_addrs_asn = asn_to_tot_addrs[asn]

        this_asn_share = 1000 * num_addrs_asn/float(top_20_tot_addrs)

        this_asn_addrs = asn_to_addrs[asn]
        random.shuffle(this_asn_addrs)

        written_ct = 0
        for addr in this_asn_addrs:

            if written_ct > this_asn_share:
                break
            
            op_fp.write("{0}|{1}\n".format(addr, asn) )

            written_ct += 1
    

# for asn in asn_to_addrs:
#     for addr in asn_to_addrs[asn]:
#         num = random.randint(1, 2)

#         if num == 2:
#             op_fp.write("{0}\n".format(addr) )

        
    
    
