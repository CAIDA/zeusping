
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

# This script will read all IP-level files in a directory (with the assumption that all files in that directory will contain one IP address per line). It will read all the addresses, add their /24s to a set and eventually print out a list of /24s

import sys
import glob
import datetime
import radix

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


inp_dir = sys.argv[1]
op_fp = open(sys.argv[2], 'w')

MUST_FIND_AS = 1

if MUST_FIND_AS == 1:
    pfx2AS_fn = '/data/routing/routeviews-prefix2as/2021/02/routeviews-rv2-20210227-1200.pfx2as.gz'
    rtree = radix.Radix()
    rnode = zeusping_helpers.load_radix_tree(pfx2AS_fn, rtree)
    s24_to_asn = {}

op_s24s = set()

# List all the files in inp_dir
ip_fnames = glob.glob('{0}/*'.format(inp_dir) )

line_ct = 0
for ip_fname in ip_fnames:
    sys.stdout.write("Reading ip_fname: {0}\n".format(ip_fname) )
    ip_fp = open(ip_fname)

    for line in ip_fp:

        line_ct += 1
        
        ip = line[:-1]

        if line_ct%100000 == 0:
            sys.stderr.write("Done with {0} lines of {1} at: {2}\n".format(line_ct, ip_fname, str(datetime.datetime.now() ) ) )
        

        parts = ip.strip().split('.')
        s24 = "{0}.{1}.{2}.0/24".format(parts[0], parts[1], parts[2])

        op_s24s.add(s24)

        if MUST_FIND_AS == 1:
            asn = 'UNK'
            s24_addr1 = "{0}.{1}.{2}.1".format(parts[0], parts[1], parts[2])
            rnode = rtree.search_best(s24_addr1)
            if rnode is None:
                asn = 'UNK'
            else:
                asn = rnode.data["origin"]

            s24_to_asn[s24] = asn

    ip_fp.close()


for s24 in op_s24s:
    op_fp.write("{0}\n".format(s24) )

if MUST_FIND_AS == 1:
    s24_to_as_fp = open("{0}_with_asn".format(sys.argv[2]), 'w')

    for s24 in s24_to_asn:
        s24_to_as_fp.write("{0}|{1}\n".format(s24, s24_to_asn[s24]) )
