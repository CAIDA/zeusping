
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

# The goal is to see why there is a mismatch between the output of quick_find_resp_addrs.py and quick_find_resp_addrs_bf24e4999c7267c71adefd904d34e77ac91f0c76.py

# Let's read all the addresses belonging to AS58224 that are responsive, according to new program

import sys

reqd_asn = sys.argv[1]
f1 = sys.argv[2]
f2 = sys.argv[3]

def find_resp_unresp(fname):
    fp = open(fname)
    resp = set()
    unresp = set()
    for line in fp:
        parts = line.strip().split()
        if len(parts) != 4:
            continue

        asn = parts[1]

        if asn != reqd_asn:
            continue
        
        ip = parts[2]

        if parts[3] == 'R':
            resp.add(ip)
        elif parts[3] == 'U':
            unresp.add(ip)

    return resp, unresp


resp_f1, unresp_f1 = find_resp_unresp(f1)
resp_f2, unresp_f2 = find_resp_unresp(f2)

diff_unresp = unresp_f2 - unresp_f1
sys.stderr.write("Diff between unresp f2 and unresp f1: {0}".format(len(diff_unresp) ) )
diff_unresp = unresp_f1 - unresp_f2
sys.stderr.write("Diff between unresp f1 and unresp f2: {0}".format(len(diff_unresp) ) )

diff_resp = resp_f1 - resp_f2
sys.stderr.write("Diff between resp f1 and resp f2: {0}".format(len(diff_resp)) )
diff_resp = resp_f2 - resp_f1
sys.stderr.write("Diff between resp f2 and resp f1: {0}".format(len(diff_resp)) )

for addr in diff_resp:
    sys.stdout.write("{0}\n".format(addr) )
