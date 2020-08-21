#!/usr/bin/env python

import sys
import glob
import shlex
import subprocess32
import os
import datetime
import json


def setup_stuff():
    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}/'.format(processed_op_dir, round_tstart, round_tend)
    args = shlex.split(mkdir_cmd)
    try:
        subprocess32.check_call(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("Mkdir failed for {0}; exiting\n".format(mkdir_cmd) )
        sys.exit(1)

    op_log_fp = open('{0}/{1}_to_{2}/process_round.log'.format(processed_op_dir, round_tstart, round_tend), 'w')
    op_log_fp.write("\nStarted reading files at: {0}\n".format(str(datetime.datetime.now() ) ) )

    return op_log_fp


def update_addr_to_resps(fname, addr_to_resps):

    wandiocat_cmd = './swift_wrapper.sh swift://zeusping-warts/{0}'.format(fname)
    print wandiocat_cmd
    args = shlex.split(wandiocat_cmd)
    try:
        ping_lines = subprocess32.check_output(args)
    except subprocess32.CalledProcessError:
        sys.stderr.write("wandiocat failed for {0}; exiting\n".format(wandiocat_cmd) )
        sys.exit(1)

    if ping_lines == None: # TODO: Test this on some file
        return
    
    line_ct = 0
    for line in ping_lines.split('\n'):

        # print line
        # sys.exit(1)

        line_ct += 1

        try:
            data = json.loads(line)
        except ValueError:
            print line
            continue

        # if 'statistics' not in data:
        #     print data

        # if 'dst' not in data:
        #     print data

        # if 'start' not in data:
        #     print data

        dst = data['dst']

        if dst not in addr_to_resps:
            addr_to_resps[dst] = [0, 0, 0, 0, 0]
        addr_to_resps[dst][0] += 1 # 0th index is sent packets

        # pinged_ts = data['start']['sec']

        resps = data['responses']

        if resps: # Apparently this way of checking for elements in a list is much faster than checking len
            this_resp = resps[0]
            icmp_type = this_resp["icmp_type"]
            icmp_code = this_resp["icmp_code"]

            if icmp_type == 0 and icmp_code == 0:
                # Responded to the ping and response is indicative of working connectivity
                addr_to_resps[dst][1] += 1 # 1st index is successful ping response
            elif icmp_type == 3 and icmp_code == 1:
                # Destination host unreachable
                addr_to_resps[dst][2] += 1 # 2nd index is Destination host unreachable
            else:
                addr_to_resps[dst][3] += 1 # 3rd index is the rest of icmp stuff. So mostly errors.

        else:

            addr_to_resps[dst][4] += 1 # 4th index is lost ping

        # is_loss = data['statistics']['loss']


############## Main begins here #####################

campaign = sys.argv[1] # CO_VT_RI/FL/iran_addrs

round_tstart = int(sys.argv[2])
round_tend = round_tstart + 600
reqd_round_num = int(round_tstart)/600

processed_op_dir = '/scratch/zeusping/data/processed_op_{0}'.format(campaign)

# Find current working directory
this_cwd = os.getcwd()

# Find the hour edge of this required round
round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)
swift_list_cmd = 'swift list zeusping-warts -p datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"))
# print swift_list_cmd
args = shlex.split(swift_list_cmd)
try:
    potential_files = subprocess32.check_output(args)
except subprocess32.CalledProcessError:
    sys.stderr.write("Swift list failed for {0}; exiting\n".format(swift_list_cmd) )
    sys.exit(1)

num_pot_files = len(potential_files)
is_setup_done = 0 # By default, we wouldn't create directories or output files; not unless there are actually warts files to process for this round. This flag keeps track of whether we've "setup" (which we would only have done had we encountered useful warts files).
addr_to_resps = {}

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

            update_addr_to_resps(fname, addr_to_resps)

            op_log_fp.write("Done reading {0}\n".format(fname) )


# If there is anything in addr_to_resps, let's write it.
if len(addr_to_resps) > 0:

    ping_aggrs_fp = open('{0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, round_tstart, round_tend), 'w')
    dst_ct = 0
    for dst in addr_to_resps:

        dst_ct += 1
        this_d = addr_to_resps[dst]
        ping_aggrs_fp.write("{0} {1} {2} {3} {4} {5}\n".format(dst, this_d[0], this_d[1], this_d[2], this_d[3], this_d[4] ) )
    
    
    #     sc_cmd = 'sc_warts2json {0}/temp_{1}_to_{2}/{3}/*.warts | python ~/zeusping/analysis/parse_eros_resps_per_addr.py {0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, round_tstart, round_tend, path_suf)
    #     sys.stderr.write("\n\n{0}\n".format(str(datetime.datetime.now() ) ) )
    #     sys.stderr.write("{0}\n".format(sc_cmd) )

    #     # NOTE: It was tricky to write the subprocess32 equivalent for the sc_cmd due to the presence of the pipes. I was also not sure what size the buffer for the pipe would be. So I just ended up using os.system() instead.
    #     # args = shlex.split(sc_cmd)
    #     # print args
    #     # try:
    #     #     subprocess32.check_call(args)
    #     # except subprocess32.CalledProcessError:
    #     #     sys.stderr.write("sc_cmd failed for {0}; exiting\n".format(sc_cmd) )
    #     #     sys.exit(1)

    #     os.system(sc_cmd)

    #     op_log_fp.write("\nFinished sc_cmd at: {0}\n".format(str(datetime.datetime.now() ) ) )

    #     # remove the temporary files
    #     rm_cmd = 'rm -rf {0}/temp_{1}_to_{2}'.format(processed_op_dir, round_tstart, round_tend)
    #     sys.stderr.write("{0}\n".format(rm_cmd) )
    #     args = shlex.split(rm_cmd)
    #     try:
    #         subprocess32.check_call(args)
    #     except subprocess32.CalledProcessError:
    #         sys.stderr.write("rm_cmd failed for {0}; exiting\n".format(sc_cmd) )
    #         sys.exit(1)

    #     sys.stderr.write("{0}\n\n".format(str(datetime.datetime.now() ) ) )

    #     op_log_fp.close()
