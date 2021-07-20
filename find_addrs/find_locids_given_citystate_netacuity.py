
#  This software is Copyright (c) 2019 The Regents of the University of
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

reqd_loc_ids = set()

line_ct = 0

reqd_ctry = sys.argv[1]
reqd_loc = sys.argv[2]
op_fname = sys.argv[3]
state_or_city = sys.argv[4]

for line in sys.stdin:
    parts = line.strip().split(',')

    line_ct += 1
    
    if (line_ct == 1):
        continue
    
    locid = int(parts[0].strip() )
    ctry_code = parts[1].strip()

    if ctry_code != reqd_ctry:
        continue
    # if (ctry_code != 'us'):
    #     continue

    if state_or_city == "CITY":
        city_code = parts[3].strip()

        if city_code == reqd_loc:
            reqd_loc_ids.add(locid)

    elif state_or_city == "STATE":
        state_code = parts[2].strip()

        if state_code == reqd_loc:
            reqd_loc_ids.add(locid)

    elif state_or_city == "CITYSTATE":
        state_code = parts[2].strip()
        city_code = parts[3].strip()

        reqd_state = sys.argv[5]
        
        if state_code == reqd_state and city_code == reqd_loc:
            reqd_loc_ids.add(locid)


op_fp = open(op_fname, 'w')
for locid in reqd_loc_ids:
    op_fp.write("{0}\n".format(locid) )
