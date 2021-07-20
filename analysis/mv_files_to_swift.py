
#  This software is Copyright (c) 2020 The Regents of the University of
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
