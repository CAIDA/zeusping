
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
import random
import os
import wandio
import datetime

# loc_to_reqd_asns contains a manual mapping of the ASes that we believe are large residential providers in different areas
# We obtained "likely residential" ASes by finding the ASes with the most addresses in the area and  then checking which of these are "known residential" using Wikipedia and the web in general.

# known_residential_asns = ['7922', '209', '7029', '5650', '20115', '7018', '22773', '701', '13977', '33363', '7155']

loc_to_reqd_asns_fname = sys.argv[1]
op_fname_pref = sys.argv[2]
num_splits = int(sys.argv[3]) # Number of files into which we should split the addresses
asn_to_addrs_path = sys.argv[4]
pfx2as_suf = sys.argv[5]

is_US = True

if len(sys.argv) == 7:
    is_US = False

mkdir_cmd = 'mkdir -p ./data/{0}'.format(op_fname_pref)
sys.stderr.write("{0}\n".format(mkdir_cmd) )
os.system(mkdir_cmd)

st_asn_to_sampling_factor = {} # The sampling factor is an integer (>=1) that decides the probability with which an address from this AS is chosen for probing

blacklist = set()
blacklist_fp = open('./data/blacklist', 'r')
for line in blacklist_fp:
    blacklist.add(line[:-1])

op_fps = {}
for sp in range(num_splits):
    op_fname = "./data/{0}/{0}_numsp{1}_sp{2}".format(op_fname_pref, num_splits, sp+1) 
    op_fps[sp] = open(op_fname, 'w')

done_addrs = set()
    
loc_to_reqd_asns_fp = open(loc_to_reqd_asns_fname)
for line in loc_to_reqd_asns_fp:

    # Each line in loc_to_reqd_asns_fp contains the reqd_asns for a state. So we are processing one state at a time.
    
    parts = line.strip().split()
    
    state = parts[0].strip()

    if state not in st_asn_to_sampling_factor:
        st_asn_to_sampling_factor[state] = {}
    
    asn_list = parts[1].strip()
    
    asns = asn_list.strip().split('-')
    
    reqd_asns = set()
    
    for asn in asns:

        asns_reqd_splits_parts = asn.strip().split(':')
        reqd_asns.add(asns_reqd_splits_parts[0])
        st_asn_to_sampling_factor[state][asns_reqd_splits_parts[0]] = int(asns_reqd_splits_parts[1])

    # print st_asn_to_sampling_factor

    if is_US == True:
        st_fname = "{0}/{1}_addrs/all_{1}_addresses{2}.pfx2as.gz".format(asn_to_addrs_path, state, pfx2as_suf)
    else:
        st_fname = parts[2].strip()
    st_fp = wandio.open(st_fname)

    st_line_ct = 0
    
    for line in st_fp:

        st_line_ct += 1

        if st_line_ct%100000 == 0:
            sys.stderr.write("Done with {0} lines of {1} at: {2}\n".format(st_line_ct, st_fname, str(datetime.datetime.now() ) ) )
        
        parts = line.strip().split('|')

        if (len(parts) != 2):
            continue

        addr = parts[0].strip()

        asn = parts[1].strip()

        # if ( (asn in reqd_asns) or (asn in known_residential_asns) ): # Perhaps we will use the latter check for surveys alone...? Not sure if it's necessary to probe 500 available addresses in Comcast in North Dakota... although it may be useful to simply find all residential addresses, no matter how few, simply to sample more ISPs. Think more about it.
        if ( (asn in reqd_asns) and (addr not in done_addrs) and (addr not in blacklist) ):

            # Added the tests below to the if condition. 
            # if addr in blacklist:
            #     continue
            
            # # NOTE: I thought that addresses would not repeat. Strangely enough, they did, so I had to add extra book-keeping below to ensure that an address that had been added before is not added again.
            # if addr in done_addrs:
            #     continue

            done_addrs.add(addr)

            if asn in st_asn_to_sampling_factor[state]:
                sampling_factor = st_asn_to_sampling_factor[state][asn]
            else:
                sampling_factor = 1
            
            sample = random.randint(1, sampling_factor)
            
            if sample == 1:
                # Address needs to be sampled. Let's decide which file to write it to based on num_splits
                file_num = random.randint(1, num_splits)

                op_fps[file_num-1].write("{0}\n".format(addr) )
