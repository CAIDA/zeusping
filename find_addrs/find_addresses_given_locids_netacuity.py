
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

# For finding US addresses, run as:
# zcat /data/external/netacuity-dumps/Edge-processed/2019-01-18.netacq-4-blocks.csv.gz | python find_addresses_given_locids_netacuity.py us_locids  /scratch/satc/active-probing/trinarkular-validation/us_addrs/ all_us_addresses.gz
# TODO: Is it possible to make this program run faster?

# For finding San Diego addressesm run a:
# zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-blocks.latest.csv.gz | python find_addresses_given_locids_netacuity.py sandiego_locids /scratch/satc/active-probing/trinarkular-validation/us_addrs/sandiego_addrs/ all_sandiego_addresses.gz

import sys
import socket
import struct
import wandio
import os

MUST_COMPRESS = 0

def int2ip(addr):
    return socket.inet_ntoa(struct.pack("!I", addr))

us_locids = set()
us_locids_fp = open(sys.argv[1], 'r')
for line in us_locids_fp:
    us_locids.add(int(line[:-1]) )


op_path = sys.argv[2]
mkdir_cmd = 'mkdir -p {0}/'.format(op_path)
sys.stderr.write("{0}\n".format(mkdir_cmd) )
os.system(mkdir_cmd)

op_fname = sys.argv[3]
if MUST_COMPRESS == 1:
    op_fp = wandio.open('{0}/{1}'.format(op_path, op_fname), 'w')
else:
    op_fp = open('{0}/{1}'.format(op_path, op_fname), 'w')
    
octets_op_fp = {}

oct3_reqd = 0

if oct3_reqd == 1:
    mkdir_cmd = 'mkdir -p {0}/addrs_by_oct3/'.format(op_path)
    sys.stderr.write("{0}\n".format(mkdir_cmd) )
    os.system(mkdir_cmd)

line_ct = 0
for line in sys.stdin:
    line_ct += 1

    if (line_ct == 1):
        continue
    
    parts = line.strip().split(',')

    locid = int(parts[2])

    if locid in us_locids:
        starting_ip_id = int(parts[0])
        ending_ip_id = int(parts[1])

        for ip in range(starting_ip_id, ending_ip_id+1):
            ip_str = int2ip(ip)
            op_fp.write("{0}\n".format(ip_str) )

            if oct3_reqd == 1:
                octs = ip_str.split('.')
                # oct1 = octs[0]
                oct3 = octs[3]            

                if oct3 not in octets_op_fp:
                    if MUST_COMPRESS == 1:
                        octets_op_fp[oct3] = wandio.open('{0}/addrs_by_oct3/{1}.gz'.format(op_path, oct3), 'w')
                    else:
                        octets_op_fp[oct3] = open('{0}/addrs_by_oct3/{1}'.format(op_path, oct3), 'w')

                octets_op_fp[oct3].write("{0}\n".format(ip_str) )
                

# Close all wandio files
op_fp.close()

if oct3_reqd == 1:
    for oct3 in octets_op_fp:
        octets_op_fp[oct3].close()
