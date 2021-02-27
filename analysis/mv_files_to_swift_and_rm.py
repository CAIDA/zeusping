
import sys
import os
import shlex
import subprocess
import glob
import time
import datetime
import wandio

# Specify wow many minutes before the current minute we should refrain from processing
MINUTE_BUFFER = 20

campaign = sys.argv[1]
inp_dir = sys.argv[2]

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

    if time.time() - round_tstart < (60 * MINUTE_BUFFER):
        continue
    
    # sys.exit(1)

    # Find all files that exist in this dir
    fils = glob.glob('{0}/*'.format(round_dir) )

    for fil in fils:

        file_name = fil.strip().split('/')[-1]

        # NOTE: Let us not try to compress the file. For some reason, many files have a compressed and uncompressed copy. So let's just upload all files to Swift.
        
        # if fil[-2:] == 'gz':
        #     object_name = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/{6}'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"), round_id, file_name)
        #     file_to_write = fil
        # else:
        #     # Compress the file
        #     gzip_cmd = 'gzip -f {0}'.format(fil)
        #     sys.stderr.write("{0}\n".format(gzip_cmd) )
        #     args = shlex.split(gzip_cmd)

        #     try:
        #         subprocess.check_call(args)
        #     except subprocess.CalledProcessError:
        #         sys.stderr.write("Gzip failed for f {0}; exiting\n".format(fil) )
        #         sys.exit(1)
        # file_to_write = "{0}.gz".format(fil)
        
        file_to_write = fil
        object_name = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/{6}'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"), round_id, file_name)
        sys.stderr.write("{0}\n".format(file_to_write) )

        # Swift upload the file
        try:
            wandio.swift.upload(file_to_write, 'zeusping-processed', object_name)
        except OSError:
            sys.stderr.write("Wandio upload failed for {0}\n".format(file_to_write) )
            continue

        # Delete the file
        os.remove(file_to_write)
        # rm_cmd = "rm {0}".format(fil)
        # sys.stderr.write("{0}\n".format(rm_cmd) )
        # args = shlex.split(rm_cmd)

        # try:
        #     subprocess.check_call(args)
        # except subprocess.CalledProcessError:
        #     sys.stderr.write("rm failed for f {0}; exiting\n".format(fil) )
        #     sys.exit(1)
        
    # rmdir this round file
    rmdir_cmd = "rmdir {0}".format(round_dir)
    
    sys.stderr.write("{0}\n".format(rmdir_cmd) )
    args = shlex.split(rmdir_cmd)

    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("rmdir failed for f {0}; exiting\n".format(fil) )
        continue
        # sys.exit(1)
    # sys.exit(1)
