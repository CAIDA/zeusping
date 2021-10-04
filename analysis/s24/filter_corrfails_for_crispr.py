
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

# Check which files are done.
# For all new files, open them, calculate how many s24s have at least 5 dropouts.
# If there are at least THRESH such s24s, write one file containing #s24s with 5 dropouts, pot_n_d, etc. in some other folder, and write one in for_crispr

import sys
import glob
import shlex
import subprocess
import os
import datetime
from collections import defaultdict
import array
import io
import wandio
import math
import numpy
import scipy
from scipy.stats import binom
import glob

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


def get_done_files(done_files_fname):
    done_f = set()
    
    fp = open(done_files_fname)
    for line in fp:
        done_f.add(line[:-1])
        
    return done_f


def find_dets_and_write_if_reqd(pot_file):
    inp_fp = open(pot_file)

    s24s_with_gt5d = 0
    s24s_with_gt5d_0r = 0
    s24s_with_gt5d_lt5r = 0
    s24s_with_gt5d_gte5r = 0
    
    line_ct = 0
    for line in inp_fp:

        line_ct += 1
        if line_ct == 1:
            # Duration string
            parts = line.strip().split('-')
            dur_str = parts[0]
            pot_n_d_str = parts[1]
            continue
        
        parts = line.strip().split()
        s24 = parts[0]
        n_p = int(parts[1]) # pinged
        n_d = int(parts[2])
        n_r = int(parts[3])

        if n_d >= 5:
            s24s_with_gt5d += 1

            if n_r == 0:
                s24s_with_gt5d_0r += 1
            elif n_r < 5:
                s24s_with_gt5d_lt5r += 1
            else:
                s24s_with_gt5d_gte5r += 1

    inp_fp.close()
    
    if s24s_with_gt5d < MIN_S24_THRESH:
        return

    pot_file_parts = pot_file.strip().split('/')
    pot_file_fname = pot_file_parts[-1]

    sys.stderr.write("{0}\n".format(pot_file_fname) )
    
    det_op_fp = open("{0}/det_data/{1}".format(pot_files_dir, pot_file_fname), "w")
    det_op_fp.write("{0}\t{1}\ts24s_with_gt5d:{2}\ts24s_with_gt5d_0r:{3}\ts24s_with_gt5d_lt5r:{4}\ts24s_with_gt5d_gte5r:{5}\n".format(dur_str, pot_n_d_str, s24s_with_gt5d, s24s_with_gt5d_0r, s24s_with_gt5d_lt5r, s24s_with_gt5d_gte5r) )
    
    for_crispr_op_fp = open("{0}/{1}".format(for_crispr_dir, pot_file_fname), "w")
    for_crispr_op_fp.write("{0}\ts24s-with-greater-than-5-dropouts:{1}\ts24s-with-greater-than-5-dropouts-and-0-responsive-addresses:{2}\ts24s-with-greater-than-5-dropouts-and-greater-than-5-responses:{3}\n".format(dur_str, s24s_with_gt5d, s24s_with_gt5d_0r, s24s_with_gt5d_gte5r) )
    
    inp_fp = open(pot_file)

    line_ct = 0
    for line in inp_fp:

        line_ct += 1

        if line_ct == 1:
            continue

        det_op_fp.write(line)
        for_crispr_op_fp.write(line)
                

def process_pot_files(pot_files_dir, done_f, done_files_fname):

    done_files_fp = open(done_files_fname, "a")
    
    pot_files = glob.glob('{0}/*'.format(pot_files_dir) )

    for pot_file in pot_files:

        if pot_file in done_f:
            continue
        
        sys.stderr.write("{0}\n".format(pot_file) )

        if "processed_for_alex" in pot_file:
            continue

        if "det_data" in pot_file:
            continue
        
        find_dets_and_write_if_reqd(pot_file)
        # sys.exit(1)

        done_files_fp.write("{0}\n".format(pot_file) )
        
    done_files_fp.close()
    

pot_files_dir = sys.argv[1]
done_files_fname = sys.argv[2]
for_crispr_dir = sys.argv[3]

# DUR_PREFLEN = len('Duration:')
MIN_S24_THRESH = 10 # How many s24s we want with at least 5 dropouts

done_f = get_done_files(done_files_fname)

process_pot_files(pot_files_dir, done_f, done_files_fname)



