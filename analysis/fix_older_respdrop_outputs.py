#!/usr/bin/env python

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
    # mv_cmd = 'mv {0}/responsive_and_dropout_addrs/{1}_to_{2}.gz {3}/{1}_to_{2}/dropout_resp_antidropout_addrs.gz'.format(src_dir, temp_tstart, temp_tend, dst_dir)
    mv_cmd = 'mv {0}/{1}_to_{2}/dropout_resp_antidropout_addrs.gz {3}/{1}_to_{2}/'.format(src_dir, temp_tstart, temp_tend, dst_dir)
    sys.stderr.write("{0}\n".format(mv_cmd) )
    args = shlex.split(mv_cmd)

    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("mv failed for cmd {0}; exiting\n".format(mv_cmd) )
        sys.exit(1)


file_ct = 0

for round_tstamp in range(round_tstart, round_tend, 600):
    run_mv(round_tstamp, round_tstamp+600)

