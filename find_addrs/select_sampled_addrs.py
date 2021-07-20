
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

# Use this script if we want to sample addresses from an aggregate (i.e., we don't want to choose all addresses, but say, want to sample about 4 of them)

import sys
import random

op_fname_pref = sys.argv[1]
num_splits = int(sys.argv[2])

op_fp = {}

for sp in range(num_splits):
    op_fname = "{0}_numsp{1}_sp{2}".format(op_fname_pref, num_splits, sp) 
    op_fp[sp] = open(op_fname, 'w')

for line in sys.stdin:
    parts = line.strip().split('|')

    if (len(parts) != 2):
        continue
    
    addr = parts[0].strip()
    asn = parts[1].strip()

    if ( (asn == '7018') or (asn == '22773') or (asn == '20001') ):    
    # if ( (asn == '20001') ):
        # num = random.randint(1, 500)
        # num = random.randint(1, 3)
        # num = random.randint(1, 10)

        num = random.randint(1, num_splits)

        op_fp[num-1].write("{0}\n".format(addr) )
        # if ( (num == 5) or (num == 6) ):
        # if ( (num == 2) ):            
        #     sys.stdout.write("{0}\n".format(addr) )

        
    
    
