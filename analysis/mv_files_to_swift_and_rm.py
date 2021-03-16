
import sys
import os
import shlex
import subprocess
import glob
import time
import datetime
import wandio
import re

# upload_to_swift_and_rm returns 1 on success and -1 on error. Remember to check for the error code!
def upload_to_swift_and_rm(fil, campaign, round_tstart, round_id):
    if IS_RDA_MODE == 1:
        file_name = "dropout_resp_antidropout_addrs.gz"
    else:
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

    retval = 1
    # Swift upload the file
    try:
        wandio.swift.upload(file_to_write, 'zeusping-processed', object_name)
    except OSError:
        sys.stderr.write("Wandio upload failed for {0}\n".format(file_to_write) )
        retval = -1
        return -1

    if retval == 1:
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

    return retval
    

# If we are uploading dropout_resp_antidropout_addrs.gz files (output of find_responsive_and_dropout_addrs_per_round.py), then we will use this mode
IS_RDA_MODE = 1

if IS_RDA_MODE == 1:
    rda_regex = re.compile('[0-9]+_to_[0-9]+.gz$')
    
# Specify how many minutes before the current minute we should refrain from processing
MINUTE_BUFFER = 20

campaign = sys.argv[1]
inp_dir = sys.argv[2]

# List all the files in inp_dir
if IS_RDA_MODE == 1:
    round_dirs = glob.glob('{0}/*_to_*.gz'.format(inp_dir) ) # NOTE: I'm expecting here that all RDA files have already been compressed    
else:
    # Default
    round_dirs = glob.glob('{0}/*_to_*'.format(inp_dir) )

for round_dir in round_dirs:

    if IS_RDA_MODE == 1:
        # We only want to upload RDA files and *not* resp_dropout_per_round, or any other type of file...
        if not rda_regex.search(round_dir):
            continue
        
        round_id = round_dir[len(inp_dir):-3] # Strip off the trailing .gz
    else:
        round_id = round_dir[len(inp_dir):]
    print round_id

    # sys.exit(1)

    round_tstart = int(round_id.strip().split('_')[0])
    round_tend = round_tstart + 600
    # print round_tstart
    # print round_tend

    round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)

    if time.time() - round_tstart < (60 * MINUTE_BUFFER):
        continue
    
    # sys.exit(1)

    if IS_RDA_MODE == 1:
        # print round_dir
        retval = upload_to_swift_and_rm(round_dir, campaign, round_tstart, round_id)
        # sys.exit(1)
        
    else:
        # Find all files that exist in this dir
        fils = glob.glob('{0}/*'.format(round_dir) )

        for fil in fils:
            retval = upload_to_swift_and_rm(fil, campaign, round_tstart, round_id)

        # rmdir this round dir
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

