
import sys
import os
import datetime
import dateutil
from dateutil.parser import parse
import subprocess
import os
import wandio
import struct
import socket
import ctypes
import shlex
import gmpy
import gc
from collections import defaultdict
from collections import namedtuple
import radix
import pyipmeta
import json

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
else:
    py_ver = 3

# find_potential_files has been mostly copied from swift_process_round_wandiocat.py
def find_potential_files(campaign, round_tstart):

    # NOTE: We may want to obtain the files for the previous 10-minute round too but *not* the next 10-minute round.
    # Suppose the tstamps in all VP files are exactly at 600s. Then we would only need to process this round num's files. This is because each file will contain the next 600s worth of pings. These are exactly the pings that we needed to have processed.
    # Suppose the tstamps in all VP files are 601s. Then we would need to process 599s worth of pings from all these files, but discard the last second. When we process the *next round*, we should process the last second from this round (otherwise the next round's first second would never get processed).
    # Suppose the tstamps in all VP files are 599s. Then we would have needed to process 599s (600 to 1199s) worth of pings but we would be discarding all of these if we are not processing the previous round. 

    # Find the hour edge of this required round
    round_tstart_dt = datetime.datetime.utcfromtimestamp(round_tstart)

    # NOTE: Perhaps replace swift_list_cmd with wandio.swift.list? I don't think it'll buy us that much additional efficiency though...
    # elems = wandio.swift.list('zeusping-warts', )

    # NOTE: If we are processing the previous round as well, may need to modify the swift_list_cmd to ensure that previous round's files will also be included. If this round is 00:00 but previous round is from the previous day at 23:50, we'll need to modify the swift list command. We'll probably just have to call the swift_list_cmd twice (once for round_tstart_dt.stuff and another for prev_round_tstart_dt.stuff, where prev_round = round - 600)
    swift_list_cmd = 'swift list zeusping-warts -p datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/'.format(campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"))
    # print swift_list_cmd
    args = shlex.split(swift_list_cmd)
    if py_ver == 2:
        try:
            potential_files = subprocess32.check_output(args)
        except subprocess32.CalledProcessError:
            sys.stderr.write("Swift list failed for {0}; exiting\n".format(swift_list_cmd) )
            sys.exit(1)
    else:
        try:
            potential_files = subprocess.check_output(args)
        except subprocess.CalledProcessError:
            sys.stderr.write("Swift list failed for {0}; exiting\n".format(swift_list_cmd) )
            sys.exit(1)
            

    return potential_files


def get_fp(campaign, reqd_t, mode, bingen, is_swift):

    if is_swift == 0:
        if bingen == 1:
            # TODO: These paths may need revisiting at some point
            if mode == 'rpr':
                inp_dir = '/scratch/zeusping/data/processed_op_{0}_testbin/'.format(campaign)
            elif mode == 'rda':
                inp_dir = '/scratch/zeusping/data/processed_op_{0}_testbintest0/'.format(campaign)
        else:
            inp_dir = '/scratch/zeusping/data/processed_op_{0}_testsimple/'.format(campaign)
        
    round_id = "{0}_to_{1}".format(reqd_t, reqd_t + zeusping_helpers.ROUND_SECS)
    
    if bingen == 1:
        if is_swift == 1:
            this_t_dt = datetime.datetime.utcfromtimestamp(reqd_t)
            if mode == 'rpr':
                reqd_t_file = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/resps_per_round.gz'.format(campaign, this_t_dt.year, this_t_dt.strftime("%m"), this_t_dt.strftime("%d"), this_t_dt.strftime("%H"), round_id)
                wandiocat_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(reqd_t_file)
            elif mode == 'rda':
                reqd_t_file = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/rda.gz'.format(campaign, this_t_dt.year, this_t_dt.strftime("%m"), this_t_dt.strftime("%d"), this_t_dt.strftime("%H"), round_id)
                wandiocat_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(reqd_t_file)
        else:
            if mode == 'rpr':
                reqd_t_file = '{0}/{1}/resps_per_round.gz'.format(inp_dir, round_id)
                wandiocat_cmd = 'wandiocat {0}'.format(reqd_t_file)
            elif mode == 'rda':
                reqd_t_file = '{0}/{1}/rda_test.gz'.format(inp_dir, round_id) # TODO: Remove the 'test' once we've finished tests
                wandiocat_cmd = 'wandiocat {0}'.format(reqd_t_file)
                
    else: # bingen != 1
        if is_swift == 1:
            pass
        else:
            if mode == 'rpr':
                reqd_t_file = "{0}/{1}/resps_per_addr.gz".format(inp_dir, round_id)
                wandiocat_cmd = 'wandiocat {0}'.format(reqd_t_file)
            elif mode == 'rda':
                reqd_t_file = "{0}/responsive_and_dropout_addrs_1610970000to1614729600/{1}.gz".format(inp_dir, round_id)
                wandiocat_cmd = 'wandiocat {0}'.format(reqd_t_file)

    sys.stderr.write("Executing {0}\n".format(wandiocat_cmd) )
    
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

    return proc.stdout
            

def print_rda_addrs_status(inp_fp, reqd_ips, reqd_t):

    for line in inp_fp:
        parts = line.strip().split()

        if len(parts) != 2:
            sys.stderr.write("Weird line length: {0}\n".format(line) )
            sys.exit(1)

        addr = parts[0].strip()

        if addr not in reqd_ips:
            continue

        status = parts[1].strip()

        sys.stdout.write("{0} {1} {2} {3}\n".format(addr, reqd_t, str(datetime.datetime.utcfromtimestamp(reqd_t)), status) )


def print_rpr_addrs_status_ascii(inp_fp, reqd_ips, reqd_t):

    for line in inp_fp:
        parts = line.strip().split()

        addr = parts[0].strip()

        if addr not in reqd_ips:
            continue

        sys.stdout.write("{0} {1} {2} {3} {4} {5} {6} {7}\n".format(ipstr, reqd_t, str(datetime.datetime.utcfromtimestamp(reqd_t)), sent_pkts, successful_resps, host_unreach, icmp_err, losses) )        
        # sys.stdout.write("{0}".format(line) )


def print_rpr_addrs_status_bin(inp_fp, reqd_ips, reqd_t):

    done_ct = 0
    while True:
        data_chunk = inp_fp.read(struct_fmt.size * 2000)

        if len(data_chunk) == 0:
            break

        gen = struct_fmt.iter_unpack(data_chunk) # TODO: This fails with Python 2 for some reason, check out why.

        for elem in gen:

            done_ct += 1
                
            ipid, sent_pkts, successful_resps, host_unreach, icmp_err, losses = elem

            ipstr = zeusping_helpers.ipint_to_ipstr(ipid)

            if ipstr not in reqd_ips:
                continue

            sys.stdout.write("{0} {1} {2} {3} {4} {5} {6} {7}\n".format(ipstr, reqd_t, str(datetime.datetime.utcfromtimestamp(reqd_t)), sent_pkts, successful_resps, host_unreach, icmp_err, losses) )


def print_warts_addrs_status(fname, reqd_ips):
    
    wandiocat_cmd = '../swift_wrapper.sh swift://zeusping-warts/{0}'.format(fname)
    sys.stderr.write("{0}\n".format(wandiocat_cmd) )
    args = shlex.split(wandiocat_cmd)

    if py_ver == 2:
        try:
            # If shell != True, then the command can be a sequence
            # proc = subprocess32.Popen(args, stdout=subprocess32.PIPE, bufsize=-1)

            # If shell == True, then the command needs to be a string and not a sequence
            proc = subprocess32.Popen(wandiocat_cmd, stdout=subprocess32.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
        except:
            sys.stderr.write("wandiocat failed for {0}; exiting\n".format(wandiocat_cmd) )
            sys.exit(1)
    else:
        try:
            # If shell != True, then the command can be a sequence
            # proc = subprocess32.Popen(args, stdout=subprocess32.PIPE, bufsize=-1)

            # If shell == True, then the command needs to be a string and not a sequence
            proc = subprocess.Popen(wandiocat_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
        except:
            sys.stderr.write("wandiocat failed for {0}; exiting\n".format(wandiocat_cmd) )
            sys.exit(1)
    
    line_ct = 0

    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            try:
                data = json.loads(line)
            except ValueError:
                sys.stderr.write("Json error for:\n {0}\n".format(line) )
                continue

            dst = data['dst']

            if dst not in reqd_ips:
                continue

            sys.stdout.write("{0}\n".format(line) )
            sys.stdout.flush()
        

def print_addrs_status(campaign, mode, bingen, is_swift, reqd_t, reqd_ips):

    if mode == 'rda' or mode == 'rpr':
        fp = get_fp(campaign, reqd_t, mode, bingen, is_swift)

        if mode == 'rda':
            # rda files are always in ascii
            print_rda_addrs_status(fp, reqd_ips, reqd_t)
        elif mode == 'rpr':
            if bingen == 1:
                print_rpr_addrs_status_bin(fp, reqd_ips, reqd_t)
            else:
                print_rpr_addrs_status_ascii(fp, reqd_ips, reqd_t)
                
    elif mode == 'warts':
        reqd_round_num = int(reqd_t)/600
        
        # Find all files that we *may* need to process
        potential_files = find_potential_files(campaign, reqd_t)
        num_pot_files = len(potential_files)

        if (num_pot_files > 0):
            if py_ver == 2:
                fname_list = potential_files.strip().split('\n')
            else:
                fname_list = potential_files.decode().strip().split('\n')

            for fname in fname_list:
                parts = fname.strip().split('.warts.gz')
                file_ctime = parts[0][-10:]
                round_num = int(file_ctime)/600
                if int(round_num) == reqd_round_num:
                    # We found a warts file that belongs to this round and needs to be processed
                    print_warts_addrs_status(fname, reqd_ips)
        

campaign = sys.argv[1]
# rda: Least detail and least time-consuming. This mode will examine if the address(es) were responsive, dropout, or anti-dropout in each round.
# rpr: Intermediate detail and time-consumption. This mode will examine, for each address, how many pings were sent in a round, how many responses, how many timeouts, how many icmp errors etc.
# warts: Maximum detail and time-consumption. This mode will print raw warts2json for an address that we care about.
mode = sys.argv[2]
bingen = int(sys.argv[3]) # Whether this file was generated by swift_process_round_wandiocat or swift_process_round_simple
is_swift = int(sys.argv[4])
start = int(sys.argv[5])
end = int(sys.argv[6])
reqd_ips_fname = sys.argv[7]

if bingen == 1:
    struct_fmt = struct.Struct("I 5H")
    buf = ctypes.create_string_buffer(struct_fmt.size * 2000)

reqd_ips = zeusping_helpers.build_setofstrs_from_file(reqd_ips_fname)

for round_tstart in range(start, end, zeusping_helpers.ROUND_SECS):
    print_addrs_status(campaign, mode, bingen, is_swift, round_tstart, reqd_ips)
