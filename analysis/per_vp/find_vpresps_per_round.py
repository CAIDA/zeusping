#!/usr/bin/env python3

#  This software is Copyright (c) 2021 The Regents of the University of
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
import datetime
import subprocess
import os
import wandio
import struct
import socket
import ctypes
import shlex
import gmpy
import gc
from collections import defaultdict
import radix
import pyipmeta
import pprint

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


def calc_vp_stats(reqd_vpnum, rpr_fp):
    while True:
        data_chunk = rpr_fp.read(struct_fmt.size * 2000) # data_chunk here is now more like a buffer

        # sys.stderr.write("{0}\n".format(len(data_chunk)) )

        # In python, once EoF is reached, we'll just read an emptry string (even for binary files), so check if chunk size is 0. This didn't work for iter_unpack for some reason, so I replaced the test for empty string with the test for len(data_chunk) == 0.            
        if len(data_chunk) == 0:
            break

        gen = struct_fmt.iter_unpack(data_chunk)

        for elem in gen:

            ipid, sent, succ, au, err, loss = elem

            ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

            # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent, succ, au, err, loss) )

            if ( (sent >> reqd_vpnum) & 1 == 1):
                vp_stats['sent'] += 1

            if ( (succ >> reqd_vpnum) & 1 == 1):
                vp_stats['succ'] += 1

            if ( (au >> reqd_vpnum) & 1 == 1):
                vp_stats['au'] += 1

            if ( (err >> reqd_vpnum) & 1 == 1):
                vp_stats['err'] += 1

            if ( (loss >> reqd_vpnum) & 1 == 1):
                vp_stats['loss'] += 1

        data_chunk = ''

    # sys.exit(1)


def get_fps(inp_dir, reqd_t, is_swift):

    if is_swift == 1:
        reqd_t_dt = datetime.datetime.utcfromtimestamp(reqd_t)
        round_id = "{0}_to_{1}".format(reqd_t, reqd_t + zeusping_helpers.ROUND_SECS)
        rpr_file = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/resps_per_round.gz'.format(campaign, reqd_t_dt.year, reqd_t_dt.strftime("%m"), reqd_t_dt.strftime("%d"), reqd_t_dt.strftime("%H"), round_id)
        wandiocat_rpr_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(rpr_file)
        vptovpnum_file = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/vp_to_vpnum.log'.format(campaign, reqd_t_dt.year, reqd_t_dt.strftime("%m"), reqd_t_dt.strftime("%d"), reqd_t_dt.strftime("%H"), round_id)
        wandiocat_vptovpnum_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(vptovpnum_file)
        
    else:
        
        vptovpnum_file = '{0}/vp_to_vpnum.log'.format(inp_dir)
        rpr_file = '{0}/resps_per_round.gz'.format(inp_dir)
        wandiocat_rpr_cmd = 'wandiocat {0}'.format(rpr_file)
        wandiocat_vptovpnum_cmd = 'wandiocat {0}'.format(vptovpnum_file)

    args = shlex.split(wandiocat_rpr_cmd)

    try:
        rpr_proc = subprocess.Popen(wandiocat_rpr_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
    except:
        sys.stderr.write("wandiocat failed for {0};\n".format(wandiocat_rpr_cmd) )
        return
        
    try:
        vptovpnum_proc = subprocess.Popen(wandiocat_vptovpnum_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
    except:
        sys.stderr.write("wandiocat failed for {0};\n".format(wandiocat_vptovpnum_cmd) )
        return

    return rpr_proc.stdout, vptovpnum_proc.stdout


def find_vpnum(fp):

    for line in fp:
        parts = line.decode().strip().split()
        vp = parts[0]

        if vp == reqd_vp:
            return int(parts[1])

    sys.stderr.write("Error! We did not find the vp\n")
    sys.exit(1)

    
campaign = sys.argv[1]
reqd_t = int(sys.argv[2])
reqd_vp = sys.argv[3]
is_swift = int(sys.argv[4]) # Whether we are reading input files from the Swift cluster or from disk

if is_swift == 0:
    inp_dir = sys.argv[5]
else:
    inp_dir = ""

struct_fmt = struct.Struct("I 5H")
buf = ctypes.create_string_buffer(struct_fmt.size * 2000)

vp_stats= defaultdict(int)

rpr_fp, vptovpnum_fp = get_fps(inp_dir, reqd_t, is_swift)
vpnum = find_vpnum(vptovpnum_fp)

calc_vp_stats(vpnum, rpr_fp)

pprint.pprint(vp_stats)
