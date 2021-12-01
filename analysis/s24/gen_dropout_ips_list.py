
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

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
else:
    py_ver = 3


def find_reqd_ips(reqd_ips_fname):

    reqd_ips = set()
    
    fp = open(reqd_ips_fname)
    for line in fp:

        parts = line.strip().split('|')
        addr = parts[0].strip()

        reqd_ips.add(addr)

    fp.close()
    return reqd_ips


def get_fp(reqd_t):
    this_t_dt = datetime.datetime.utcfromtimestamp(reqd_t)
    round_id = "{0}_to_{1}".format(reqd_t, reqd_t + zeusping_helpers.ROUND_SECS)
    reqd_t_file = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/rda_multiround.gz'.format(campaign, this_t_dt.year, this_t_dt.strftime("%m"), this_t_dt.strftime("%d"), this_t_dt.strftime("%H"), round_id)
    wandiocat_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(reqd_t_file)

    args = shlex.split(wandiocat_cmd)

    if py_ver == 2:
        try:
            proc = subprocess32.Popen(wandiocat_cmd, stdout=subprocess32.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
        except:
            sys.stderr.write("wandiocat failed for {0};\n".format(wandiocat_cmd) )
            return
    else:
        try:
            proc = subprocess.Popen(wandiocat_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
        except:
            sys.stderr.write("wandiocat failed for {0};\n".format(wandiocat_cmd) )
            return

    return proc.stdout


campaign = sys.argv[1]
reqd_t = int(sys.argv[2])

ip_fp = get_fp(reqd_t)

reqd_ips_fname = sys.argv[3]
reqd_ips = find_reqd_ips(reqd_ips_fname)

for line in ip_fp:

    parts = line.decode().strip().split()

    addr = parts[0].strip()

    # print(addr)

    if addr not in reqd_ips:
        continue

    status = int(parts[1])

    if status == 0:
        sys.stdout.write("{0}|0\n".format(addr) )
    elif status == 1:
        sys.stdout.write("{0}|1\n".format(addr) )
    

