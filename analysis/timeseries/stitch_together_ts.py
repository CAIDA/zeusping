
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

# NOTE: Before running this script, make sure to increase the limit of open file descriptors on zeus

import sys
import os
import shlex
import subprocess
import datetime
from collections import defaultdict


if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
else:
    py_ver = 3

    
def read_ts_file(this_t, ts_fname, is_swift):

    if is_swift == 1:
        wandiocat_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(ts_fname)
    else:
        wandiocat_cmd = 'wandiocat {0}'.format(ts_fname)
    sys.stderr.write("{0}\n".format(wandiocat_cmd) )

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

    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            parts = line.strip().split('|')
            full_name = parts[0]

            # if "projects.zeusping.test1.routing" in full_name:
            #     asn = parts[1]
            #     if asn not in op_fps:
            #         op_fps[asn] = open("{0}/pinged_resp_per_round/pinged_resp_per_round_AS{1}".format(op_dir, asn), 'w')
            #     op_fps[asn].write("{0} {1} {2}\n".format(this_t, parts[2], parts[3]) )
            # elif "projects.zeusping.test1.geo.netacuity" in full_name:
            fqdn = full_name[offset:]
            # print fqdn

            if this_t not in tstamp_to_vals:
                tstamp_to_vals[this_t] = defaultdict(dict)

            if is_rda == 0:
                n_p = int(parts[2])
                tstamp_to_vals[this_t][fqdn]['n_p'] = n_p

                n_r = int(parts[3])
                tstamp_to_vals[this_t][fqdn]['n_r'] = n_r

                n_r_prev = n_r
                prev_t = this_t - 600
                if prev_t in tstamp_to_vals:
                    if fqdn in tstamp_to_vals[prev_t]:
                        n_r_prev = tstamp_to_vals[prev_t][fqdn]['n_r']

                n_d = n_r_prev - n_r
                if n_d > 0:
                    n_a = 0
                else:
                    n_a = abs(n_d)
                    n_d = 0

                if fqdn not in op_fps:
                    op_fps[fqdn] = open("{0}/pinged_resp_per_round_{1}to{2}/pinged_resp_per_round_{3}".format(op_dir, tstart, tend, fqdn), 'w')

                op_fps[fqdn].write("{0}|{1}|{2}|{3}|{4}|{5}\n".format(this_t, str(datetime.datetime.utcfromtimestamp(this_t)), n_p, n_d, n_r, n_a) )

            else: # is_rda

                n_d = int(parts[2])
                tstamp_to_vals[this_t][fqdn]['n_d'] = n_d
                
                n_r = int(parts[3])
                tstamp_to_vals[this_t][fqdn]['n_r'] = n_r
                
                n_a = int(parts[4])
                tstamp_to_vals[this_t][fqdn]['n_a'] = n_a

                if fqdn not in op_fps:
                    op_fps[fqdn] = open("{0}/rda_per_round_{1}to{2}/rda_per_round_{3}".format(op_dir, tstart, tend, fqdn), 'w')

                op_fps[fqdn].write("{0}|{1}|{2}|{3}|{4}\n".format(this_t, str(datetime.datetime.utcfromtimestamp(this_t)), n_d, n_r, n_a) )


tstart = int(sys.argv[1])
tend = int(sys.argv[2])
campaign = sys.argv[3]
op_dir = sys.argv[4]
is_swift = int(sys.argv[5])
is_rda = int(sys.argv[6])

# offset = len("projects.zeusping.test1.geo.netacuity.")
offset = len("projects.zeusping.test1.")

if is_rda == 0:
    mkdir_cmd = 'mkdir -p {0}/pinged_resp_per_round_{1}to{2}/'.format(op_dir, tstart, tend)
else:
    mkdir_cmd = 'mkdir -p {0}/rda_per_round_{1}to{2}/'.format(op_dir, tstart, tend)
    
args = shlex.split(mkdir_cmd)
if py_ver == 2:
    try:
        subprocess32.check_call(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)

else:
    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)


op_fps = {}
tstamp_to_vals = {}

for this_t in range(tstart, tend, 600):

    round_id = "{0}_to_{1}".format(this_t, this_t + 600)
    this_t_dt = datetime.datetime.utcfromtimestamp(this_t)

    if is_swift == 1:
        ts_fname = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/ts.gz'.format(campaign, this_t_dt.year, this_t_dt.strftime("%m"), this_t_dt.strftime("%d"), this_t_dt.strftime("%H"), round_id)
    else:
        # Let's assume that op_dir is the same as the inp_dir
        if is_rda == 0:
            ts_fname = "{0}/{1}/ts.gz".format(op_dir, round_id)
        else:
            # TODO: Remove the "test" once we have this finalized
            ts_fname = "{0}/{1}/ts_rda_test".format(op_dir, round_id)

    sys.stderr.write("{0}\n".format(ts_fname) )
    # sys.exit(1)

    read_ts_file(this_t, ts_fname, is_swift)
