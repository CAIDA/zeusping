#!/usr/bin/env python

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
import glob
import shlex
import subprocess
import os
import datetime


src_dir = sys.argv[1]
dst_dir = sys.argv[2]

mkdir_cmd = 'mkdir -p {0}'.format(dst_dir)
# sys.stderr.write("{0}\n".format(mkdir_cmd) )
args = shlex.split(mkdir_cmd)
try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
    sys.exit(1)

round_tstart = int(sys.argv[3])
round_tend = int(sys.argv[4])


def run_mv(temp_tstart, temp_tend):
    # mv_cmd = 'mv {0}/temp_{1}_to_{2}/resps_per_addr {0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, temp_tstart, temp_tend)

    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}'.format(dst_dir, temp_tstart, temp_tend)
    # sys.stderr.write("{0}\n".format(mkdir_cmd) )
    args = shlex.split(mkdir_cmd)
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)

    # mv_cmd = 'mv {0}/{1}_to_{2}/resps_per_addr.gz {3}/{1}_to_{2}/'.format(src_dir, temp_tstart, temp_tend, dst_dir)
    mv_cmd = 'mv {0}/{1}_to_{2}/process_round.log.gz {3}/{1}_to_{2}/'.format(src_dir, temp_tstart, temp_tend, dst_dir)
    # sys.stderr.write("{0}\n".format(mv_cmd) )
    args = shlex.split(mv_cmd)

    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("mv failed for f {0}; exiting\n".format(f) )
        sys.exit(1)



file_ct = 0

for round_tstamp in range(round_tstart, round_tend, 600):
    run_mv(round_tstamp, round_tstamp+600)

