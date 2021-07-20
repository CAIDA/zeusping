
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

# Given a set of addresses in one month (m1; the more recent one) and a set of addresses in another (m2; the older one), find the addresses that were in m2 but not in m1. Lookup the pfx2as in m2 to determine which ASes these addresses belonged to.

import sys
import wandio
from collections import defaultdict

def populate_set(fname):
    s = set()
    fp = open(fname)

    for line in fp:
        s.add(line[:-1])

    return s

m1_addrs = populate_set(sys.argv[1])

print "Number of m1 addrs is: {0}".format(len(m1_addrs) )

m2_addrs = populate_set(sys.argv[2])

print "Number of m2 addrs is: {0}".format(len(m2_addrs) )

intersect = m1_addrs & m2_addrs

print "Intersect size is: {0}".format(len(intersect))

diff = m2_addrs - m1_addrs

print "Diff size is {0}".format(len(diff) )

m2_pfx2as_fp = wandio.open(sys.argv[3])

asn_to_diff_ct = defaultdict(int)
for line in m2_pfx2as_fp:
    parts = line.strip().split('|')
    addr = parts[0].strip()

    if addr in diff:
        asn = parts[1].strip()
        asn_to_diff_ct[asn] += 1

sorted_tuple = sorted(asn_to_diff_ct.items(), key=lambda kv: kv[1])
for entry in sorted_tuple:
    sys.stderr.write("{0} {1}\n".format(entry[0], entry[1]))
    
