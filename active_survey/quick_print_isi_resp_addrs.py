#!/usr/bin/env python

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
import pyipmeta
from collections import namedtuple
import wandio
import datetime


def print_isi_addrs(num_hists):

    line_ct = 0

    for line in sys.stdin:

        line_ct += 1

        if line_ct == 1:
            continue

        if line_ct%1000000 == 0:
            sys.stderr.write("{0} isi_hitlist lines read at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )
        
        # if line_ct > 1000:
        #     sys.exit(1)
        
        parts = line.strip().split()

        ip = parts[1].strip()

        hist = parts[2].strip()
        # hist = hist[-1] # NOTE: Hack to make this faster temporarily

        hex_int = int(hist, 16)

        resp = 0

        for i in range(num_hists):
            if hex_int & 1 == 1:
                resp = 1

            hex_int = hex_int >> 1

        # print ip, hist, hex_int, resp

        if resp == 1:
            resp_fp.write("{0}\n".format(ip) )
        elif resp == 0:
            unresp_fp.write("{0}\n".format(ip) )


resp_fp = open(sys.argv[1], 'w')
unresp_fp = open(sys.argv[2], 'w')
# num_hists is the number of historical censuses we will consider to determine if an address was respsonsive
num_hists = int(sys.argv[3])
print_isi_addrs(num_hists)
    
    
