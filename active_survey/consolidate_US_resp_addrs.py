
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
from collections import defaultdict

inp_fp = open(sys.argv[1])

asn_to_status = {}
for line in inp_fp:
    parts = line.strip().split('|')
    asn = parts[1].strip()

    if asn not in asn_to_status:
        asn_to_status[asn] = {"resp" : 0, "unresp" : 0}
    
    resp = int(parts[2].strip() )
    unresp = int(parts[3].strip() )
    asn_to_status[asn]["resp"] += resp
    asn_to_status[asn]["unresp"] += unresp


op_fp = open(sys.argv[2], 'w')
for asn in asn_to_status:
    tot_resp = asn_to_status[asn]["resp"]
    tot_unresp = asn_to_status[asn]["unresp"]

    resp_pct = (tot_resp/float(tot_resp + tot_unresp)) * 100.0

    op_fp.write("US|{0}|{1}|{2}|{3}|{4:.4f}\n".format(asn, tot_resp, tot_unresp, (tot_resp + tot_unresp), resp_pct) )
