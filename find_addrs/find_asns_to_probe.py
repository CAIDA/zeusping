
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

regionasn_to_status_fname = sys.argv[1]
regionasn_to_status_fp = open(regionasn_to_status_fname)

op_fname = sys.argv[2]
sampling_factor = int(sys.argv[3])

region_to_reqd_asns = {}
for line in regionasn_to_status_fp:
    parts = line.strip().split()

    region = parts[0].strip()
    asn = parts[1].strip()
    
    num_resp = int(parts[2].strip())
    num_unresp = int(parts[3].strip())

    pct_resp = float(parts[5].strip())
    
    if pct_resp > 10 and num_resp > 1000:
        if region not in region_to_reqd_asns:
            region_to_reqd_asns[region] = set()
            
        region_to_reqd_asns[region].add(asn)


op_fp = open(op_fname, "w")
for region in region_to_reqd_asns:

    asn_str = ''
    for asn in region_to_reqd_asns[region]:
        asn_str += '{0}:{1}-'.format(asn, sampling_factor)

    asn_str = asn_str[:-1]
    op_fp.write("{0} {1}\n".format(region, asn_str) )
