
import sys
import os
import shlex
import subprocess
import glob
import time
import datetime
import wandio

campaign = sys.argv[1]
inp_dir = sys.argv[2]
reqd_fname = sys.argv[3] # resps_per_addr, process_round etc.

# List all the files in inp_dir
round_dirs = glob.glob('{0}/*_to_*'.format(inp_dir) )

for round_dir in round_dirs:

    round_id = round_dir[len(inp_dir):]
    print round_id

    # sys.exit(1)

    round_tstart = int(round_id.strip().split('_')[0])
    round_tend = round_tstart + 600
    print round_tstart
    print round_tend

    round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)
    
    # sys.exit(1)

    # Check if reqd_fname (resps_per_addr or resps_per_addr.gz) exists in this dir
    fils = glob.glob('{0}/{1}*'.format(round_dir, reqd_fname) )

    for fil in fils:

        file_name = fil.strip().split('/')[-1]
        
        if fil[-2:] == 'gz':
            object_name = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/{6}'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"), round_id, file_name)
            file_to_write = fil
        else:
            # Compress the file
            gzip_cmd = 'gzip {0}'.format(fil)
            sys.stderr.write("{0}\n".format(gzip_cmd) )
            args = shlex.split(gzip_cmd)

            try:
                subprocess.check_call(args)
            except subprocess.CalledProcessError:
                sys.stderr.write("Gzip failed for f {0}; exiting\n".format(fil) )
                sys.exit(1)

            object_name = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/{6}.gz'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"), round_id, file_name)
            file_to_write = "{0}.gz".format(fil)


        # Swift upload the file
        wandio.swift.upload(file_to_write, 'zeusping-processed', object_name)
            
    
    # sys.exit(1)
