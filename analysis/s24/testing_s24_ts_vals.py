
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

ip_fname = sys.argv[1]
ip_fp = open(ip_fname)

reqd_s24 = sys.argv[2]

reqd_s24_addr_part = reqd_s24.strip().split('/')[0]
# reqd_s24_addr_part = reqd_s24_parts[0]

# Convert s24 file into ip addr
reqd_s24_ipid = struct.unpack("!I", socket.inet_aton(reqd_s24_addr_part))[0]


for line in ip_fp:
    parts = line.strip().split('|')

    s24 = int(parts[0].strip() )

    if s24 != reqd_s24_ipid:
    # if s24 != '142.202.88.0/24':
        continue

    s24_to_dets = defaultdict(set)
    
    d_addrs = int(parts[1])
    zeusping_helpers.find_addrs_in_s24_with_status(reqd_s24, d_addrs, 'd', s24_to_dets)
    
    r_addrs = int(parts[2])
    zeusping_helpers.find_addrs_in_s24_with_status(reqd_s24, r_addrs, 'r', s24_to_dets)
    
    a_addrs = int(parts[3])
    zeusping_helpers.find_addrs_in_s24_with_status(reqd_s24, a_addrs, 'a', s24_to_dets)    
    # sys.exit(1)

statuses = ['d', 'r', 'a']
for status in statuses:
    if status in s24_to_dets:
        for addr in s24_to_dets[status]:
            sys.stdout.write("{0} {1}\n".format(addr, status) )
            # op_fp.write("{0} {1} {2}\n".format(oct4, s24_to_set_bits[oct4], status) )
    
