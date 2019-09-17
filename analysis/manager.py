
import sys
import glob
import os
import datetime

processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'

def cp_and_gunzip(f, temp_tstart, temp_tend):
    cp_cmd = 'cp {0} {1}/temp_{2}_to_{3}/'.format(f, processed_op_dir, temp_tstart, temp_tend)
    sys.stderr.write("{0}\n".format(cp_cmd) )
    os.system(cp_cmd)

    f_suf = f.strip().split('/')[-1]

    gunzip_cmd = 'gunzip {0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, f_suf)
    sys.stderr.write("{0}\n".format(gunzip_cmd) )
    os.system(gunzip_cmd)

    # Check file size and delete if necessary
    statinfo = os.stat('{0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, f_suf[:-3]) )
    if statinfo.st_size == 0:
        rm_cmd = 'rm {0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, f_suf[:-3])
        sys.stderr.write("{0}\n".format(rm_cmd) )
        os.system(rm_cmd)


tstart = int(sys.argv[1])
tend = int(sys.argv[2])

temp_tstart = tstart
temp_tend = temp_tstart + 600
mkdir_cmd = 'mkdir -p {0}/temp_{1}_to_{2}/'.format(processed_op_dir, temp_tstart, temp_tend)
os.system(mkdir_cmd)

file_ct = 0
unsorted_files = glob.glob('/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/op_randsorted_colorado_4M/opaws*')
sorted_files = sorted(unsorted_files, key = lambda file: os.path.getctime(file))
for f in sorted_files:
    parts = f.strip().split('.')
    this_t = int(parts[1])

    if this_t < temp_tstart: # So that we can process arbitrary periods of time instead of always starting from the beginning
        continue

    if this_t < temp_tend:
        cp_and_gunzip(f, temp_tstart, temp_tend)    
    else:
        # Time to process all of these files
        sc_cmd = '/nmhomes/ramapad/scamper_2019/bin/sc_warts2json {0}/temp_{1}_to_{2}/*.warts | python parse_eros_resps_per_addr.py {0}/temp_{1}_to_{2}/resps_per_addr'.format(processed_op_dir, temp_tstart, temp_tend)
        sys.stderr.write("\n\n{0}\n".format(str(datetime.datetime.now() ) ) )
        sys.stderr.write("{0}\n".format(sc_cmd) )

        os.system(sc_cmd)
        sys.stderr.write("{0}\n\n".format(str(datetime.datetime.now() ) ) )

        # Create a new directory to process the next 10 minutes
        temp_tstart = temp_tstart + 600
        temp_tend = temp_tstart + 600

        if temp_tend > tend:
            print temp_tend
            print tend
            sys.exit(1)

        mkdir_cmd = 'mkdir -p {0}/temp_{1}_to_{2}/'.format(processed_op_dir, temp_tstart, temp_tend)
        os.system(mkdir_cmd)

        cp_and_gunzip(f, temp_tstart, temp_tend)

    file_ct += 1

