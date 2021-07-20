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

reqd_dir = sys.argv[1]

round_tstart = int(sys.argv[2])
round_tend = round_tstart + 600

# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_{0}'.format(reqd_dir)
processed_op_dir = '/scratch/zeusping/data/processed_{0}'.format(reqd_dir)


def cp_and_gunzip(f, temp_tstart, temp_tend):
    cp_cmd = 'cp {0} {1}/temp_{2}_to_{3}/'.format(f, processed_op_dir, temp_tstart, temp_tend)
    # sys.stderr.write("{0}\n".format(cp_cmd) )
    args = shlex.split(cp_cmd)

    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("cp failed for f {0}; exiting\n".format(f) )
        sys.exit(1)

    f_suf = f.strip().split('/')[-1]

    gunzip_cmd = 'gunzip {0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, f_suf)
    # sys.stderr.write("{0}\n".format(gunzip_cmd) )
    args = shlex.split(gunzip_cmd)
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Gunzip failed for f {0}; exiting\n".format(f) )
        sys.exit(1)

    # Check file size and delete if necessary
    statinfo = os.stat('{0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, f_suf[:-3]) )
    if statinfo.st_size == 0:
        rm_cmd = 'rm {0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, f_suf[:-3])
        # sys.stderr.write("{0}\n".format(rm_cmd) )
        args = shlex.split(rm_cmd)
        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            sys.stderr.write("rm failed for f {0}; exiting\n".format(f) )
            sys.exit(1)


file_ct = 0
op_log_fp = open('{0}/{1}_to_{2}/process_round.log'.format(processed_op_dir, round_tstart, round_tend), 'w')

mkdir_cmd = 'mkdir -p {0}/temp_{1}_to_{2}/'.format(processed_op_dir, round_tstart, round_tend)
args = shlex.split(mkdir_cmd)
try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
    sys.exit(1)


op_log_fp.write("\nStarted copying files at: {0}\n".format(str(datetime.datetime.now() ) ) )
reqd_files = glob.glob('{0}/{1}_to_{2}/opaws*.gz'.format(processed_op_dir, round_tstart, round_tend) )
for f in reqd_files:
    cp_and_gunzip(f, round_tstart, round_tend)
op_log_fp.write("Finished copying files at: {0}\n".format(str(datetime.datetime.now() ) ) )

# Time to process all of these files
op_log_fp.write("\nStarted sc_cmd at: {0}\n".format(str(datetime.datetime.now() ) ) )
sc_cmd = 'sc_warts2json {0}/temp_{1}_to_{2}/*.warts | python parse_eros_resps_per_addr.py {0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, round_tstart, round_tend)
sys.stderr.write("\n\n{0}\n".format(str(datetime.datetime.now() ) ) )
sys.stderr.write("{0}\n".format(sc_cmd) )

# NOTE: It was tricky to write the subprocess equivalent for the sc_cmd due to the presence of the pipes. I was also not sure what size the buffer for the pipe would be. So I just ended up using os.system() instead.
# args = shlex.split(sc_cmd)
# print args
# try:
#     subprocess.check_call(args)
# except subprocess.CalledProcessError:
#     sys.stderr.write("sc_cmd failed for {0}; exiting\n".format(sc_cmd) )
#     sys.exit(1)

os.system(sc_cmd)

op_log_fp.write("\nFinished sc_cmd at: {0}\n".format(str(datetime.datetime.now() ) ) )

# remove the temporary files
rm_cmd = 'rm -rf {0}/temp_{1}_to_{2}'.format(processed_op_dir, round_tstart, round_tend)
sys.stderr.write("{0}\n".format(rm_cmd) )
args = shlex.split(rm_cmd)
try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("rm_cmd failed for {0}; exiting\n".format(sc_cmd) )
    sys.exit(1)

sys.stderr.write("{0}\n\n".format(str(datetime.datetime.now() ) ) )

op_log_fp.close()
