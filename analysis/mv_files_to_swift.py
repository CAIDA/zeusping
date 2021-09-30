# TODO: Make compatible with Python3

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


def upload_file_to_swift_and_rm(fpath, fname, campaign, round_tstart, round_id):
    round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)

    file_to_write = fpath
    object_name = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/{6}'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"), round_id, fname)
    sys.stderr.write("File to upload to Swift: {0}\n".format(file_to_write) )

    retval = 1
    # Swift upload the file
    try:
        wandio.swift.upload(file_to_write, 'zeusping-processed', object_name)
    except OSError:
        sys.stderr.write("Wandio upload failed for {0}\n".format(file_to_write) )
        retval = -1
        return -1

    if retval == 1:
        # Delete the file, but *only* if upload was successful
        os.remove(file_to_write)
    
    return retval
    
    
def process_all_rounds(campaign, inp_dir, reqd_fnames, reqd_tstart, reqd_tend):

    # List all the files in inp_dir
    round_dirs = glob.glob('{0}/*_to_*'.format(inp_dir) )

    for round_dir in round_dirs:

        round_id = round_dir[len(inp_dir):]

        round_tstart = int(round_id.strip().split('_')[0])
        
        if time.time() - round_tstart < (60 * MINUTE_BUFFER):
            continue

        if round_tstart < reqd_tstart or round_tstart >= reqd_tend:
            continue

        round_tend = round_tstart + 600
        sys.stderr.write("{0}\n".format(round_id) )
        
        for fname in reqd_fnames:
            fpath = "{0}/{1}".format(round_dir, fname)
            retval = upload_file_to_swift_and_rm(fpath, fname, campaign, round_tstart, round_id)

            if retval == -1:
                # No big deal, continue to try to upload other files. We can always mop up the remaining files later.
                pass
            

def obtain_reqd_fnames(reqd_fname):
    reqd_fnames = []
    if "," in reqd_fname:
        parts = reqd_fname.strip().split(',')
        for part in parts:
            reqd_fnames.append(part)

    return reqd_fnames

    
campaign = sys.argv[1]
inp_dir = sys.argv[2]
reqd_fname = sys.argv[3] # resps_per_addr, process_round etc.
reqd_tstart = int(sys.argv[4])
reqd_tend = int(sys.argv[5])

reqd_fnames = obtain_reqd_fnames(reqd_fname)

# print(reqd_fnames)
# sys.exit(1)

# Specify how many minutes before the current minute we should refrain from processing
MINUTE_BUFFER = 20

process_all_rounds(campaign, inp_dir, reqd_fnames, reqd_tstart, reqd_tend)

