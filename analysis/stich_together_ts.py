
# NOTE: Before running this script, make sure to increase the limit of open file descriptors on zeus

import sys
import os
import shlex
import subprocess
import datetime


if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
    from sc_warts import WartsReader
else:
    py_ver = 3

    
def read_ts_file(this_t, ts_fname):
    
    wandiocat_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(ts_fname)
    sys.stderr.write("{0}\n".format(wandiocat_cmd) )

    args = shlex.split(wandiocat_cmd)

    if py_ver == 2:
        try:
            proc = subprocess32.Popen(wandiocat_cmd, stdout=subprocess32.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
        except:
            sys.stderr.write("wandiocat failed for {0};\n".format(wandiocat_cmd) )
            return
    else:
        try:
            proc = subprocess.Popen(wandiocat_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
        except:
            sys.stderr.write("wandiocat failed for {0};\n".format(wandiocat_cmd) )
            return

    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            parts = line.strip().split('|')
            full_name = parts[0]
            if "projects.zeusping.test1.routing" in full_name:
                asn = parts[1]
                if asn not in op_fps:
                    op_fps[asn] = open("{0}/pinged_resp_per_round/pinged_resp_per_round_AS{1}".format(op_dir, asn), 'w')
                op_fps[asn].write("{0} {1} {2}\n".format(this_t, parts[2], parts[3]) )
            elif "projects.zeusping.test1.geo.netacuity" in full_name:
                fqdn = parts[0][offset:]
                # print fqdn
                if fqdn not in op_fps:
                    op_fps[fqdn] = open("{0}/pinged_resp_per_round/pinged_resp_per_round_{1}".format(op_dir, fqdn), 'w')
                op_fps[fqdn].write("{0} {1} {2}\n".format(this_t, parts[2], parts[3]) )


tstart = int(sys.argv[1])
tend = int(sys.argv[2])
campaign = sys.argv[3]
op_dir = sys.argv[4]
is_swift = int(sys.argv[5])

offset = len("projects.zeusping.test1.geo.netacuity.")

mkdir_cmd = 'mkdir -p {0}/pinged_resp_per_round/'.format(op_dir)
args = shlex.split(mkdir_cmd)
if py_ver == 2:
    try:
        subprocess32.check_call(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)

else:
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)


op_fps = {}

for this_t in range(tstart, tend, 600):

    round_id = "{0}_to_{1}".format(this_t, this_t + 600)
    this_t_dt = datetime.datetime.utcfromtimestamp(this_t)
    
    ts_fname = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/ts.gz'.format(campaign, this_t_dt.year, this_t_dt.strftime("%m"), this_t_dt.strftime("%d"), this_t_dt.strftime("%H"), round_id)

    read_ts_file(this_t, ts_fname)

    
    
