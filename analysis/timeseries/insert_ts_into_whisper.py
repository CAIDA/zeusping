
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
import _pytimeseries
import glob
from collections import defaultdict
import datetime
import shlex
import subprocess

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


def set_keyval(key, val):
    e_key = key.encode('utf-8')
    idx = kp.get_key(e_key)
    if idx is None:
        idx = kp.add_key(e_key)
    kp.set(idx, val)


def build_tstamp_to_vals(inp_dir, tstamp_to_vals):
    # List all the files in inp_dir
    ts_files = glob.glob('{0}/*'.format(inp_dir) )

    for ts_file in ts_files:
        # sys.stderr.write("{0}\n".format(ts_file) )

        if "UNK" in ts_file:
            # sys.stderr.write("{0}\n".format(ts_file) )        
            continue

        if "routing.asn" in ts_file:
            if is_US == 1:
                # Let's not write AS-level statistics for any campaigns in the U.S.
                # sys.stderr.write("{0}\n".format(ts_file) )
                continue

        ts_file_parts = ts_file.strip().split('_')
        fqdn = "{0}.{1}".format(PREF, ts_file_parts[-1])

        # sys.stderr.write("{0}\n".format(fqdn) )

        ts_file_fp = open(ts_file)

        for line in ts_file_fp:
            parts = line.strip().split('|')

            ts = int(parts[0])

            if ts not in tstamp_to_vals:
                tstamp_to_vals[ts] = {}
            
            n_p = int(parts[2])
            key = "{0}.pinged_addr_cnt".format(fqdn)
            tstamp_to_vals[ts][key] = n_p
            
            n_r = int(parts[4])
            r_key = "{0}.echoresponse_addr_cnt".format(fqdn)
            tstamp_to_vals[ts][r_key] = n_r

            n_r_prev = n_r
            prev_ts = ts - 600
            if prev_ts in tstamp_to_vals:
                if r_key in tstamp_to_vals[prev_ts]:
                    n_r_prev = tstamp_to_vals[prev_ts][r_key]
            
            n_d = n_r_prev - n_r
            
            if n_d >= 0:
                key = "{0}.disrupted_addr_cnt".format(fqdn)
                tstamp_to_vals[ts][key] = n_d
                key = "{0}.antidisrupted_addr_cnt".format(fqdn)
                tstamp_to_vals[ts][key] = 0
            else:
                key = "{0}.antidisrupted_addr_cnt".format(fqdn)
                tstamp_to_vals[ts][key] = abs(n_d)
                key = "{0}.disrupted_addr_cnt".format(fqdn)
                tstamp_to_vals[ts][key] = 0
            
        # sys.exit(1)


PREF = "projects.zeusping.test1"

inp_dir = sys.argv[1]
# If we are processing the US, let us not write timeseries files for ASes. We do not want to do this because various ZeusPing campaigns would have pinged addresses in the same AS (7922 is pinged in LA_MS_AL_AR_FL_CT and also in CA_ME). If we insert TS values for each AS from these campaigns, they will get clobbered.
is_US = int(sys.argv[2])


ts = _pytimeseries.Timeseries()

# try to get ascii by name
# print "Asking for ASCII backend by name:"
be = ts.get_backend_by_name("ascii")
# print "Got backend: %d, %s (%s)" % (be.id, be.name, be.enabled)
# print

# try to enable the ascii backend with options
# print "Enabling ASCII backend (with ignored options):"
# print ts.enable_backend(be, "ignored options")
# print be
# print

# enable the ascii backend
# print "Enabling ASCII backend:"
ts.enable_backend(be)
# print be
# print

# try to set a single value
# print "Setting a single value:"
# print "Should look like: a.test.key 12345 532051200"
# print ts.set_single("a.test.key", 12345, 532051200)
# print

kp = ts.new_keypackage(reset=True)
# print kp


tstamp_to_vals = {}
build_tstamp_to_vals(inp_dir, tstamp_to_vals)

for tstamp in tstamp_to_vals:
    for key in tstamp_to_vals[tstamp]:
        # NOTE: Comment the following if we want all time series values
        # if ( ("pinged_addr_cnt" in key) or ("echoresponse_addr_cnt" in key) ):
        #     continue
        set_keyval(key, tstamp_to_vals[tstamp][key])
        
    kp.flush(tstamp)
