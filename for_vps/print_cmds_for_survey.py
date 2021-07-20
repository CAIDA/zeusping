
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

campaign = sys.argv[1]
num_splits = 2

# vps = [
#     {"loc" : "atlanta1", "plat" : "lin"},
#     {"loc" : "sf1", "plat" : "doc"},
#     {"loc" : "tor1", "plat" : "doc"},
#     {"loc" : "nyc1", "plat" : "doc"},
#     ]

# vps = [
#     {"loc" : "fremont2", "plat" : "lin"},
#     {"loc" : "dallas2", "plat" : "doc"},
#     {"loc" : "newark2", "plat" : "doc"},
#     ]

vps = [
    {"loc" : "fremont3", "plat" : "lin"},
    {"loc" : "dallas3", "plat" : "lin"},
    {"loc" : "newark3", "plat" : "lin"},
    ]

for vp in vps:
    for i in range(num_splits):
        # cmd = "sudo /home/ramapad/scamper_2019/bin/sc_erosprober -U scamper_{0}.sock -a {1}sp{0} -o /home/ramapad/zeusping/for_testing/op_{1}/{2}{0}{3} -I 1800 -R 600 -c 'ping -c 1'".format(i+1, campaign, vp["plat"], vp["loc"])
        # For testing Scamper's -I with larger values
        cmd = "sudo /home/ramapad/scamper_2019/bin/sc_erosprober -U scamper_{0}.sock -a {1}sp{0} -o /home/ramapad/zeusping/for_testing/op_{1}/survey{2}{0}{3} -I 1800 -R 600 -c 'ping -c 1'".format(i+1, campaign, vp["plat"], vp["loc"])
        sys.stderr.write("{0}\n".format(cmd) )
