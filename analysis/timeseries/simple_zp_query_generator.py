
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

fqdn = sys.argv[1]
alias = sys.argv[2]

reqd_addr_cnts = ["pinged_addr_cnt", "echoresponse_addr_cnt", "responsive_addr_cnt", "dropout_addr_cnt", "antidropout_addr_cnt", "disrupted_addr_cnt", "antidisrupted_addr_cnt"] # For testing swift_process_round_wandiocat.py by comparing against swift_process_round_simple.py
# reqd_addr_cnts = ["pinged_addr_cnt", "echoresponse_addr_cnt",  "disrupted_addr_cnt", "antidisrupted_addr_cnt"]
# reqd_addr_cnts = ["echoresponse_addr_cnt"]

query = ''

for addr_cnt in reqd_addr_cnts:

    this_query = '''
      alias(
        keepLastValue(
          sumSeries(
            {0}.{1},
          )
        ),
        "{2} {1}"
      )'''.format(fqdn, addr_cnt, alias)

    query += '{0},'.format(this_query)


sys.stdout.write("{0}\n".format(query) )
