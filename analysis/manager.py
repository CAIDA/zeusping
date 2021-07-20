
#  This software is Copyright (c) 2019 The Regents of the University of
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

# NOTE NOTE NOTE: This script is obsolete.
# This script has a bug. It expects that sorting by getctime will sort all files. However, some files couldn't be rsync'd on the first attempt and I scp'd them over at a later stage---these files will have a different ctime
# We solved this problem by having a pre-processing step where we copied files over from the massive current dir into a new dir structure (like we did using  manager.py, except that we would delete the copied files). 

import sys
import glob
import os
import datetime


tstart = int(sys.argv[1])
tend = int(sys.argv[2])

reqd_dir = sys.argv[3]

# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'
# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_CO_accra_CA/'
# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_CO_VT_RI/'
# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/temp_processed_op_CO_accra_CA_awso/'
processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_{0}'.format(reqd_dir)


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


temp_tstart = tstart
temp_tend = temp_tstart + 600
mkdir_cmd = 'mkdir -p {0}/temp_{1}_to_{2}/'.format(processed_op_dir, temp_tstart, temp_tend)
os.system(mkdir_cmd)

file_ct = 0

# The following is for testing specific vps
# unsorted_files = glob.glob('/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/op_CO_accra_CA/opawso*')

# The following is for the production run:
# unsorted_files = glob.glob('/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/op_randsorted_colorado_4M/opaws*')
# unsorted_files = glob.glob('/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/op_CO_accra_CA/opaws*')
unsorted_files = glob.glob('/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/{0}/opaws*'.format(reqd_dir) )

# TODO: There is probably a way I can sort files without taking the ctime into account. The filenames have timestamps. Sort by those timestamps.
# TODO: Maintain a dictionary which keeps track of the latest file seen for each vp. My condition: if temp_tend > tend is currently incorrect. Consider when opawso produced a file at timestamp 1577725795 and when opawsv produced a fileat timestamp 1577725804 but the ctime for opawsv was earlier than that for opawso. The current code will skip processing opawso. 
vp_to_latest_time = {"opawsv" : 0, "opawso" : 0, "opawsc"  : 0}
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

