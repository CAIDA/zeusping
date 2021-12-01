
import sys
import os
import shlex
import glob
import time
import datetime
import wandio


def comp(fil, file_name):
    
    # If the file isn't already compressed, compress it
    if file_name[-2:] == 'gz':
        was_compressed = 1
    else:
        was_compressed = 0        
        # Compress this file
        gzip_cmd = 'gzip {0}'.format(fil)
        sys.stderr.write("{0}\n".format(gzip_cmd) )
        args = shlex.split(gzip_cmd)    

        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            sys.stderr.write("Gzip failed for f {0}; exiting\n".format(fil) )
            sys.exit(1)

    return was_compressed
            

def process_fil(fil, curr_time, until_time):
    # print fil
    file_name = fil.strip().split('/')[-1]
    
    parts = fil.strip().split('.')
    # print parts
    
    file_pref = parts[0].strip().split('/')[-1]
    # print file_pref
    
    this_t = int(parts[1])

    # Skip files that are too recent
    if this_t > until_time:
        break

    # Compress the file if it's not already compressed
    # NOTE: Due to some edge-conditions (if this script had failed after
    # compression but before the mv, for example), it is possible that we may
    # find a compressed file in zp_output_dir. We will handle this edge-case
    # by using the was_compressed flag.
    was_compressed = comp(fil, fil_name)

    round_number = this_t/600
    round_tstart = round_number * 600
    round_tend = round_tstart + 600

    round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)

    if was_compressed == 1:
        # The file had already been compressed. No need to add the 'gz' extension
        dst_fpath = '{0}/datasource=zeusping/campaign={1}/year={2}/month={3}/day={4}/hour={5}/{6}'.format(rsync_dir, campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"), file_name)
        src_fpath = fil
    else:
        # We just compressed the file. Add the 'gz' extension
        dst_fpath = '{0}/datasource=zeusping/campaign={1}/year={2}/month={3}/day={4}/hour={5}/{6}.gz'.format(rsync_dir, campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"), file_name)
        src_fpath = "{0}.gz".format(fil)

    try:
        shutil.move(src_fpath, dst_fpath)
    except:
        sys.stderr.write("Failed mv for {0}\n".format(src_fpath) )
        sys.exit(1)


zp_output_dir = sys.argv[1]
rsync_dir = sys.argv[2]
campaign = sys.argv[3]

mkdir_cmd = 'mkdir -p {0}/'.format(rsync_dir)
args = shlex.split(mkdir_cmd)
try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
    sys.exit(1)

    
while True:

    curr_time = time.time()
    until_time = curr_time - 60*15 # Get all gz files that were produced before 15 minutes    

    unsorted_files = glob.glob('{0}/*warts*'.format(zp_output_dir) )
    sorted_files = sorted(unsorted_files, key = lambda f: os.path.getctime(f) )
    
    for fil in sorted_files:

        process_fil(fil, curr_time, until_time)
        
        
