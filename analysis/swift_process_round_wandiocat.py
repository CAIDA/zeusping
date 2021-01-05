#!/usr/bin/env python

import sys
import glob
import shlex
import subprocess32
import subprocess
import os
import datetime
import json
from collections import defaultdict
import io


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
        # If shell != True, then the command can be a sequence
        # proc = subprocess32.Popen(args, stdout=subprocess32.PIPE, bufsize=-1)
        
        # If shell == True, then the command needs to be a string and not a sequence
        proc = subprocess32.Popen(wandiocat_cmd, stdout=subprocess32.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
    except:
        sys.stderr.write("wandiocat failed for {0}; exiting\n".format(wandiocat_cmd) )
        sys.exit(1)
        
    # try:
    #     ping_lines = subprocess32.check_output(args)
    # except subprocess32.CalledProcessError:
    #     sys.stderr.write("wandiocat failed for {0}; exiting\n".format(wandiocat_cmd) )
    #     sys.exit(1)

    # if ping_lines == None: # TODO: Test this on some file
    #     return
    
    line_ct = 0

    # ping_lines = proc.communicate()[0]
    # for line in ping_lines.split('\n'):
    # while True:
    # for line in proc.stdout:
    # for line in io.open(proc.stdout.fileno()):
    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
    # while proc.poll() is None:
    
            # print line
            # sys.exit(1)

            # line = proc.stdout.readline()

            # if not line:
            #     break

            line_ct += 1

            try:
                data = json.loads(line)
            except ValueError:
                print line
                continue

            dst = data['dst']

            # NOTE: defaultdict should take care of the following... let's see if this gets us savings
            # if dst not in addr_to_resps:
            #     addr_to_resps[dst] = [0, 0, 0, 0, 0]
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

            
    proc.wait() # Wait for the subprocess to exit

    # remaining_ping_lines = proc.communicate()[0]
    # for line in remaining_ping_lines.splitlines():
    #     line_ct += 1
    


############## Main begins here #####################

campaign = sys.argv[1] # CO_VT_RI/FL/iran_addrs

round_tstart = int(sys.argv[2])
round_tend = round_tstart + 600
reqd_round_num = int(round_tstart)/600

# NOTE: We may want to obtain the files for the previous 10-minute round too but *not* the next 10-minute round.
# Suppose the tstamps in all VP files are exactly at 600s. Then we would only need to process this round num's files. This is because each file will contain the next 600s worth of pings. These are exactly the pings that we needed to have processed.
# Suppose the tstamps in all VP files are 601s. Then we would need to process 599s worth of pings from all these files, but discard the last second. When we process the *next round*, we should process the last second from this round (otherwise the next round's first second would never get processed).
# Suppose the tstamps in all VP files are 599s. Then we would have needed to process 599s (600 to 1199s) worth of pings but we would be discarding all of these if we are not processing the previous round. 

# NOTE: Change output dir for each test!
processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_test'.format(campaign)

# Find current working directory
this_cwd = os.getcwd()

# Find the hour edge of this required round
round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)

# NOTE: Perhaps replace swift_list_cmd with wandio.swift.list? I don't think it'll buy us that much additional efficiency though...
# elems = wandio.swift.list('zeusping-warts', )

# NOTE: If we are processing the previous round as well, may need to modify the swift_list_cmd to ensure that previous round's files will also be included. If this round is 00:00 but previous round is from the previous day at 23:50, we'll need to modify the swift list command. We'll probably just have to call the swift_list_cmd twice (once for round_tstart_dt.stuff and another for prev_round_tstart_dt.stuff, where prev_round = round - 600)
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
# addr_to_resps = {}
addr_to_resps = defaultdict(lambda : [0, 0, 0, 0, 0])

# TODO: Think about whether we would need to read files generated a minute or two before/after current round
if (num_pot_files > 0):

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

            op_log_fp.write("Done reading {0} at: {1}\n".format(fname, str(datetime.datetime.now() ) ) )


# If there is anything in addr_to_resps, let's write it.
if len(addr_to_resps) > 0:

    ping_aggrs_fp = open('{0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, round_tstart, round_tend), 'w')
    dst_ct = 0
    for dst in addr_to_resps:

        dst_ct += 1
        this_d = addr_to_resps[dst]
        ping_aggrs_fp.write("{0} {1} {2} {3} {4} {5}\n".format(dst, this_d[0], this_d[1], this_d[2], this_d[3], this_d[4] ) )

op_log_fp.write("Done with round {0}_to_{1} at: {2}\n".format(round_tstart, round_tend, str(datetime.datetime.now() ) ) )
