
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

# This is a script to select ~10M Iranian addresses out of 12.5M total

import sys
import random
import os

op_fname_pref = sys.argv[1]
num_splits = int(sys.argv[2]) # Number of files into which we should split the addresses

mkdir_cmd = 'mkdir -p ./data/{0}'.format(op_fname_pref)
sys.stderr.write("{0}\n".format(mkdir_cmd) )
os.system(mkdir_cmd)

op_fps = {}
for sp in range(num_splits):
    op_fname = "./data/{0}/{0}_numsp{1}_sp{2}".format(op_fname_pref, num_splits, sp+1) 
    op_fps[sp] = open(op_fname, 'w')

done_addrs = set()

asn_to_addrs = {}

line_ct = 0
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
        asn_to_addrs[asn] = set()
    asn_to_addrs[asn].add(addr)


for asn in asn_to_addrs:
    for addr in asn_to_addrs[asn]:

        num = random.randint(1, 5)

        if ( (num != 5) and (addr not in done_addrs) ):

            # Address needs to be sampled. Let's decide which file to write it to based on num_splits
            file_num = random.randint(1, num_splits)

            op_fps[file_num-1].write("{0}\n".format(addr) )
            
        done_addrs.add(addr)
        
    
    
