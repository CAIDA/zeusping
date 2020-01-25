
import sys
import glob
import time
import calendar
import datetime
import shlex
import subprocess


reqd_dir = sys.argv[1]
processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_{0}'.format(reqd_dir)


def run_mv(f, this_t):

    round_number = this_t/600
    round_tstart = round_number * 600
    round_tend = round_tstart + 600


    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}/'.format(processed_op_dir, round_tstart, round_tend)
    args = shlex.split(mkdir_cmd)
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)


    mv_cmd = 'mv {0} {1}/{2}_to_{3}/'.format(f, processed_op_dir, round_tstart, round_tend)
    sys.stderr.write("{0}\n".format(mv_cmd) )
    args = shlex.split(mv_cmd)
    
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Mv failed for f {0}; exiting\n".format(f) )
        sys.exit(1)


file_ct = 0

reqd_time = time.time() - 3600 # 1 hour before current time
# reqd_time = 1577181600

unsorted_files = glob.glob('/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/{0}/opaws*'.format(reqd_dir) )
for f in unsorted_files:
    parts = f.strip().split('.')
    this_t = int(parts[1])

    if this_t < reqd_time:
        run_mv(f, this_t)

    file_ct += 1

