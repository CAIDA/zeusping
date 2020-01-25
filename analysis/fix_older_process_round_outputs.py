#!/usr/bin/env python

import sys
import glob
import shlex
import subprocess
import os
import datetime

reqd_dir = sys.argv[1]

round_tstart = int(sys.argv[2])
round_tend = int(sys.argv[3])

processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_{0}'.format(reqd_dir)


def run_mv(temp_tstart, temp_tend):
    mv_cmd = 'mv {0}/temp_{1}_to_{2}/resps_per_addr {0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, temp_tstart, temp_tend)
    sys.stderr.write("{0}\n".format(mv_cmd) )
    args = shlex.split(mv_cmd)

    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("mv failed for f {0}; exiting\n".format(f) )
        sys.exit(1)


file_ct = 0

for round_tstamp in range(round_tstart, round_tend, 600):
    run_mv(round_tstamp, round_tstamp+600)

