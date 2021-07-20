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
import glob
import shlex
import subprocess32
import os
import datetime


def setup_stuff():
    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}/'.format(processed_op_dir, round_tstart, round_tend)
    args = shlex.split(mkdir_cmd)
    try:
        subprocess32.check_call(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)

    mkdir_cmd = 'mkdir -p {0}/temp_{1}_to_{2}/'.format(processed_op_dir, round_tstart, round_tend)
    args = shlex.split(mkdir_cmd)
    try:
        subprocess32.check_call(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)

    op_log_fp = open('{0}/{1}_to_{2}/process_round.log'.format(processed_op_dir, round_tstart, round_tend), 'w')
    op_log_fp.write("\nStarted copying files at: {0}\n".format(str(datetime.datetime.now() ) ) )

    return op_log_fp


def find_potential_files(round_tstart_dt):
    swift_list_cmd = 'swift list zeusping-warts -p datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"))
    # print swift_list_cmd
    args = shlex.split(swift_list_cmd)
    try:
        potential_files = subprocess32.check_output(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("Swift list failed for {0}; exiting\n".format(swift_list_cmd) )
        sys.exit(1)

    return potential_files    
    

def download_and_gunzip(f, temp_tstart, temp_tend, path_suf, warts_fname_suf):

    # cd_cmd = 'cd 
    # print cd_cmd
    # args = shlex.split(cd_cmd)
    # try:
    #     subprocess32.check_call(args)
    # except subprocess32.CalledProcessError:
    #     sys.stderr.write("cd failed for f {0}; exiting\n".format(f) )
    #     sys.exit(1)

    try:
        this_path = '{0}/temp_{1}_to_{2}/'.format(processed_op_dir, temp_tstart, temp_tend)
        os.chdir(this_path)
    except:
        sys.stderr.write("Change dir to temp failed\n")
        sys.exit(1)

    dload_cmd = 'swift download zeusping-warts -p {0}'.format(f)
    # print dload_cmd
    args = shlex.split(dload_cmd)
    try:
        subprocess32.check_call(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("dload failed for f {0}; exiting\n".format(f) )
        sys.exit(1)

    try:
        this_path = '{0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, path_suf)
        # print this_path
        os.chdir(this_path)
    except:
        sys.stderr.write("Change dir to Swift dir failed\n")
        sys.exit(1)
        
    # args = shlex.split(new_cd_cmd)
    # try:
    #     subprocess32.check_call(args)
    # except subprocess32.CalledProcessError:
    #     sys.stderr.write("New cd failed for f {0}; exiting\n".format(f) )
    #     sys.exit(1)

    f_suf = f.strip().split('/')[-1]

    gunzip_cmd = 'gunzip {0}'.format(warts_fname_suf)
    # sys.stderr.write("{0}\n".format(gunzip_cmd) )
    args = shlex.split(gunzip_cmd)
    try:
        subprocess32.check_call(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("Gunzip failed for f {0}; exiting\n".format(f) )
        sys.exit(1)

    # Check file size and delete if necessary
    statinfo = os.stat('{0}/temp_{1}_to_{2}/{3}/{4}'.format(processed_op_dir, temp_tstart, temp_tend, path_suf, warts_fname_suf[:-3]) )
    # print "Statinfo coming up"
    # print statinfo.st_size
    if statinfo.st_size == 0:
        rm_cmd = 'rm {0}/temp_{1}_to_{2}/{3}'.format(processed_op_dir, temp_tstart, temp_tend, f_suf[:-3])
        # sys.stderr.write("{0}\n".format(rm_cmd) )
        args = shlex.split(rm_cmd)
        try:
            subprocess32.check_call(args)
        except subprocess32.CalledProcessError:
            sys.stderr.write("rm failed for f {0}; exiting\n".format(f) )
            sys.exit(1)


def main():
    ############## Main begins here #####################
    
    reqd_round_num = int(round_tstart)/600

    # Find the hour edge of this required round
    round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)

    # Find all files that we *may* need to process
    potential_files = find_potential_files(round_tstart_dt)
    
    # Find current working directory
    this_cwd = os.getcwd()

    num_pot_files = len(potential_files)
    is_setup_done = 0 # By default, we wouldn't create directories or output files; not unless there are actually warts files to process for this round. This flag keeps track of whether we've "setup" (which we would only have done had we encountered useful warts files).

    if (num_pot_files > 0):

        path_suf = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"))    
        for fname in potential_files.strip().split('\n'):
            # print fname
            parts = fname.strip().split('.warts.gz')
            # print parts
            file_ctime = parts[0][-10:]
            # print file_ctime

            round_num = int(file_ctime)/600

            if round_num == reqd_round_num:
                # We found a warts file that belongs to this round and needs to be processed

                if is_setup_done == 0:
                    op_log_fp = setup_stuff()
                    is_setup_done = 1

                warts_fname_suf = fname.strip().split('/')[-1]
                # print warts_fname_suf
                download_and_gunzip(fname, round_tstart, round_tend, path_suf, warts_fname_suf)

        print "is_setup_done: {0}".format(is_setup_done)
        if is_setup_done == 1:
            op_log_fp.write("Finished copying files at: {0}\n".format(str(datetime.datetime.now() ) ) )

            # Time to process all of these files
            op_log_fp.write("\nStarted sc_cmd at: {0}\n".format(str(datetime.datetime.now() ) ) )

            os.chdir(this_cwd)

            # NOTE: Open resps_per_addr instead of resps_per_addr.gz for temporary testing purposes only
            # sc_cmd = 'sc_warts2json {0}/temp_{1}_to_{2}/{3}/*.warts | python ~/zeusping/analysis/parse_eros_resps_per_addr.py {0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, round_tstart, round_tend, path_suf)        
            sc_cmd = 'sc_warts2json {0}/temp_{1}_to_{2}/{3}/*.warts | python ~/zeusping/analysis/parse_eros_resps_per_addr.py {0}/{1}_to_{2}/resps_per_addr.gz'.format(processed_op_dir, round_tstart, round_tend, path_suf)

            sys.stderr.write("\n\n{0}\n".format(str(datetime.datetime.now() ) ) )
            sys.stderr.write("{0}\n".format(sc_cmd) )

            # NOTE: It was tricky to write the subprocess32 equivalent for the sc_cmd due to the presence of the pipes. I was also not sure what size the buffer for the pipe would be. So I just ended up using os.system() instead.
            # args = shlex.split(sc_cmd)
            # print args
            # try:
            #     subprocess32.check_call(args)
            # except subprocess32.CalledProcessError:
            #     sys.stderr.write("sc_cmd failed for {0}; exiting\n".format(sc_cmd) )
            #     sys.exit(1)

            os.system(sc_cmd)

            op_log_fp.write("\nFinished sc_cmd at: {0}\n".format(str(datetime.datetime.now() ) ) )

            # remove the temporary files
            rm_cmd = 'rm -rf {0}/temp_{1}_to_{2}'.format(processed_op_dir, round_tstart, round_tend)
            sys.stderr.write("{0}\n".format(rm_cmd) )
            args = shlex.split(rm_cmd)
            try:
                subprocess32.check_call(args)
            except subprocess32.CalledProcessError:
                sys.stderr.write("rm_cmd failed for {0}; exiting\n".format(sc_cmd) )
                sys.exit(1)

            sys.stderr.write("{0}\n\n".format(str(datetime.datetime.now() ) ) )

            op_log_fp.close()

            
############## Read command line args and call main() #####################
campaign = sys.argv[1] # CO_VT_RI/FL/iran_addrs

round_tstart = int(sys.argv[2])
round_tend = round_tstart + 600

# processed_op_dir = '/scratch/zeusping/data/processed_op_{0}'.format(campaign)
processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_testsimple'.format(campaign)

main()
