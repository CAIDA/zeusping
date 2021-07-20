
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
import time
import calendar
import datetime
import shlex
import subprocess


reqd_dir = sys.argv[1]
processed_op_dir = sys.argv[2]


def run_mv(f, this_t):

    round_number = this_t/600
    round_tstart = round_number * 600
    round_tend = round_tstart + 600


    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}/'.format(processed_op_dir, round_tstart, round_tend)
    # sys.stderr.write("{0}\n".format(mkdir_cmd) )    
    args = shlex.split(mkdir_cmd)
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)


    mv_cmd = 'mv {0} {1}/{2}_to_{3}/'.format(f, processed_op_dir, round_tstart, round_tend)
    # sys.stderr.write("{0}\n".format(mv_cmd) )
    args = shlex.split(mv_cmd)
    
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Mv failed for f {0}; exiting\n".format(f) )
        sys.exit(1)


file_ct = 0

reqd_time = time.time() - 3600 # 1 hour before current time
# reqd_time = 1577181600

unsorted_files = glob.glob('{0}/opaws*'.format(reqd_dir) )

# sys.stderr.write("{0}\n".format(unsorted_files) )

for f in unsorted_files:
    parts = f.strip().split('.')
    this_t = int(parts[1])

    if this_t < reqd_time:
        run_mv(f, this_t)

    file_ct += 1

