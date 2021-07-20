
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
import subprocess
import shlex

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
    from sc_warts import WartsReader
else:
    py_ver = 3

start_round_epoch = int(sys.argv[1])
end_round_epoch = int(sys.argv[2])
test_mode = sys.argv[3] # 'simple' to test non-binary. 'bin' to test binary.

# These are rounds with known anomalies that we want to skip the diff test
iggy_rounds = set()
iggy_rounds.add(1617497400)
iggy_rounds.add(1617561600)

# for round_tstamp in range(1617495600, 1617753000, 600):
for round_tstamp in range(start_round_epoch, end_round_epoch, zeusping_helpers.ROUND_SECS):

    # Skip rounds where there are known anomalies (that are not really bugs but just anomalies)
    if mode == 'bin':
        if round_tstamp in iggy_rounds:
            continue
    
    # First sort the new format file
    if test_mode == 'simple':
        new_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/{0}_to_{1}/rda_test.gz".format(round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
        temp_new_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/{0}_to_{1}/temp_rda_test".format(round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    else:
        new_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testbin/{0}_to_{1}/rda_test.gz".format(round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
        temp_new_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testbin/{0}_to_{1}/temp_rda_test".format(round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
        
    cmd = "zcat {0} | sort > {1}".format(new_format_fname, temp_new_format_fname)
    print(cmd)
    # args = shlex.split(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)

    # with proc.stderr:
    #     for line in iter(proc.stderr.readline, b''):
    #         print(line)

    # Next sort the old format file
    old_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/responsive_and_dropout_addrs_1617495000to1617753600/{0}_to_{1}.gz".format(round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    temp_old_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/responsive_and_dropout_addrs_1617495000to1617753600/temp_{0}_to_{1}".format(round_tstamp, round_tstamp + zeusping_helpers.ROUND_SECS)
    cmd = "zcat {0} | sort > {1}".format(old_format_fname, temp_old_format_fname)
    print(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)

    # Run diff
    diff_cmd = "diff {0} {1} | wc -l".format(temp_old_format_fname, temp_new_format_fname)
    print(diff_cmd)
    # proc = subprocess.Popen(diff_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    # proc.wait()
    # print("Return code: {0}".format(proc.returncode) )
    # with proc.stdout:
    #     for line in iter(proc.stdout.readline, b''):
    #         print(line)
    proc = subprocess.run(diff_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True) # Note: No need to explicitly called wait if we call run()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)
    if proc.stdout != b'0\n':
        sys.stderr.write("Outputs did not match!\n")
        sys.exit(1)
    
    
    # delete old format file
    rm_old_cmd = "rm {0}".format(temp_old_format_fname)
    print(rm_old_cmd)
    proc = subprocess.Popen(rm_old_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)

    # Delete new format file
    rm_new_cmd = "rm {0}".format(temp_new_format_fname)
    print(rm_new_cmd)
    proc = subprocess.Popen(rm_new_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)
    
    # sys.exit(1)
    
