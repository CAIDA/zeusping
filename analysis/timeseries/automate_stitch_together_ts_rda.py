
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
import glob
import shlex
import subprocess
import os
import datetime
import json
from collections import defaultdict
import array
import io
import struct
import socket
import wandio
import math
import pprint

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


campaign = sys.argv[1]
rda_path = sys.argv[2]
is_swift = 0
is_rda = 1

week_tstamps = [
    [1615075200, 1617494400], # Mar
    [1617494400, 1619913600], # Apr
    [1619913600, 1622937600], # May
    [1622937600, 1625356800], # Jun
]

rda_statuses = [0, 1] # 0 is for sr, 1 is for mr

# Increase the ulimit
ulimit_cmd = "ulimit -S -n 100000"
sys.stderr.write("{0}\n".format(ulimit_cmd) )
os.system(ulimit_cmd)

for rda_status in rda_statuses: 
    for week_tstamp_pair in week_tstamps:
        week_tstamp_start = week_tstamp_pair[0]
        week_tstamp_end = week_tstamp_pair[1]

        sys.stderr.write("Beginning run for {0} {1} {2} at {3}\n".format(rda_status, week_tstamp_start, week_tstamp_end, str(datetime.datetime.now() ) ) )
        cmd = """python stitch_together_ts.py {0} {1} {2} {3} {4} {5} {6}""".format(week_tstamp_start, week_tstamp_end, campaign, rda_path, is_swift, is_rda, rda_status)
        sys.stderr.write("{0}\n".format(cmd) )
        args = shlex.split(cmd)
        # os.system(cmd)

        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            sys.stderr.write("cmd failed for {0} {1} {2}; exiting\n".format(rda_status, week_tstamp_start, week_tstamp_end) )
            # continue
            sys.exit(1)

