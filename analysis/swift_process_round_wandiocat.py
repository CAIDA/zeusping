#!/usr/bin/env python3

import sys
import glob
import shlex
import subprocess
import os
import datetime
import json
from collections import defaultdict
import array
import io
import struct
import socket

if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
    from sc_warts import WartsReader
else:
    py_ver = 3

# No-op version of @profile, so that we don't need to keep uncommenting @profile when we're not using kernprof
try:
    # Python 2
    import __builtin__ as builtins
except ImportError:
    # Python 3
    import builtins

try:
    builtins.profile
except AttributeError:
    # No line profiler, provide a pass-through version
    def profile(func): return func
    builtins.profile = profile

@profile
def setup_stuff():
    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}/'.format(processed_op_dir, round_tstart, round_tend)
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

    op_log_fp = open('{0}/{1}_to_{2}/process_round.log'.format(processed_op_dir, round_tstart, round_tend), 'w')
    op_log_fp.write("\nStarted reading files at: {0}\n".format(str(datetime.datetime.now() ) ) )

    vp_to_vpnum_fp = open('{0}/{1}_to_{2}/vp_to_vpnum.log'.format(processed_op_dir, round_tstart, round_tend), 'w')

    return op_log_fp, vp_to_vpnum_fp

@profile
def find_potential_files():

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

@profile        
def update_addr_to_resps(fname, addr_to_resps, vpnum):

    wandiocat_cmd = './swift_wrapper.sh swift://zeusping-warts/{0}'.format(fname)
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
        
        
    # try:
    #     ping_lines = subprocess32.check_output(args)
    # except subprocess32.CalledProcessError:
    #     sys.stderr.write("wandiocat failed for {0}; exiting\n".format(wandiocat_cmd) )
    #     sys.exit(1)

    # if ping_lines == None: # TODO: Test this on some file
    #     return
    
    line_ct = 0
    mask = 1<<vpnum

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

            # Let's not count the lines since we're not doing anything with them
            # line_ct += 1

            try:
                data = json.loads(line)
            except ValueError:
                sys.stderr.write("Json error for:\n {0}\n".format(line) )
                continue

            dst = data['dst']

            # Don't increment. Bit shift to vp_num position and set bit to 1
            addr_to_resps[dst][0] |= (mask)  # 0th index is sent packets

            # pinged_ts = data['start']['sec']

            resps = data['responses']

            if resps: # Apparently this way of checking for elements in a list is much faster than checking len
                this_resp = resps[0]
                icmp_type = this_resp["icmp_type"]
                icmp_code = this_resp["icmp_code"]

                if icmp_type == 0 and icmp_code == 0:
                    # Responded to the ping and response is indicative of working connectivity
                    addr_to_resps[dst][1] |= (mask) # 1st index is successful ping response
                elif icmp_type == 3 and icmp_code == 1:
                    # Destination host unreachable
                    addr_to_resps[dst][2] |= (mask) # 2nd index is Destination host unreachable
                else:
                    addr_to_resps[dst][3] |= (mask) # 3rd index is the rest of icmp stuff. So mostly errors.

            else:

                addr_to_resps[dst][4] |= (mask) # 4th index is lost ping

            
    proc.wait() # Wait for the subprocess to exit

    # remaining_ping_lines = proc.communicate()[0]
    # for line in remaining_ping_lines.splitlines():
    #     line_ct += 1


def readwarts_update_addr_to_resps(fname, addr_to_resps, vpnum):

    wandiocat_cmd = 'wandiocat swift://zeusping-warts/{0}'.format(fname)
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

    mask = 1<<vpnum

    ct = 0
    with proc.stdout:
        w = WartsReader(proc.stdout, verbose=False)
        while True:
            (flags, hops) = w.next()
            if flags == False: break

            
            dst = flags['dstaddr']

            # Don't increment. Bit shift to vp_num position and set bit to 1
            addr_to_resps[dst][0] |= (mask)  # 0th index is sent packets

            ct += 1
            
            if len(hops) > 0:
                icmp = hops[0]['icmp']
                icmp_type = (icmp >> 8) & 0xFF
                icmp_code = (icmp >> 0) & 0xFF

                if icmp_type == 0 and icmp_code == 0:
                    # Responded to the ping and response is indicative of working connectivity
                    addr_to_resps[dst][1] |= (mask) # 1st index is successful ping response
                elif icmp_type == 3 and icmp_code == 1:
                    # Destination host unreachable
                    addr_to_resps[dst][2] |= (mask) # 2nd index is Destination host unreachable
                else:
                    addr_to_resps[dst][3] |= (mask) # 3rd index is the rest of icmp stuff. So mostly errors.

            else:
                
                addr_to_resps[dst][4] |= (mask) # 4th index is lost ping

    proc.wait() # Wait for the subprocess to exit
    
    
@profile
def write_addr_to_resps(addr_to_resps, processed_op_dir, round_tstart, round_tend, op_log_fp):
    if len(addr_to_resps) > 0:

        if write_bin == 0:
            op_fname = '{0}/{1}_to_{2}/resps_per_round_ascii'.format(processed_op_dir, round_tstart, round_tend)
            ping_aggrs_fp = open(op_fname, 'w')
            dst_ct = 0
            for dst in addr_to_resps:

                # dst_ct += 1 # Since we don't use this variable, let's not update it. We get to save 6 seconds according to kernprof
                this_d = addr_to_resps[dst]
                ping_aggrs_fp.write("{0} {1} {2} {3} {4} {5}\n".format(dst, this_d[0], this_d[1], this_d[2], this_d[3], this_d[4] ) )

            ping_aggrs_fp.close()

        else:
            op_fname = '{0}/{1}_to_{2}/resps_per_round'.format(processed_op_dir, round_tstart, round_tend)
            ping_aggrs_fp = open(op_fname, 'wb')

            for dst in addr_to_resps:
                try:
                    ipid = struct.unpack("!I", socket.inet_aton(dst))[0]
                except socket.error:
                    continue

                

        # Compress the file
        gzip_cmd = 'gzip {0}'.format(op_fname)
        # sys.stderr.write("{0}\n".format(gzip_cmd) )
        args = shlex.split(gzip_cmd)

        if py_ver == 2:
            try:
                subprocess32.check_call(args)
            except subprocess32.CalledProcessError:
                sys.stderr.write("Gzip failed for f {0}; exiting\n".format(f) )
                sys.exit(1)
        else:
            try:
                subprocess.check_call(args)
            except subprocess.CalledProcessError:
                sys.stderr.write("Gzip failed for f {0}; exiting\n".format(f) )
                sys.exit(1)
            

        # TODO: Upload to Swift

        # TODO: Delete temporary file after verifying that the upload completed

    op_log_fp.write("Done with round {0}_to_{1} at: {2}\n".format(round_tstart, round_tend, str(datetime.datetime.now() ) ) )

@profile    
def main():
    ############## Main begins here #####################

    reqd_round_num = int(round_tstart)/600

    # Find all files that we *may* need to process
    potential_files = find_potential_files()

    num_pot_files = len(potential_files)
    is_setup_done = 0 # By default, we wouldn't create directories or output files; not unless there are actually warts files to process for this round. This flag keeps track of whether we've "setup" (which we would only have done had we encountered useful warts files).
    # addr_to_resps = {}
    # addr_to_resps = defaultdict(lambda : [0, 0, 0, 0, 0])
    addr_to_resps = defaultdict(lambda : array.array('H', [0, 0, 0, 0, 0]) )

    # TODO: Think about whether we would need to read files generated a minute or two before/after current round
    if (num_pot_files > 0):

        vpnum = 0
        vp_to_vpnum = {}

        if py_ver == 2:
            fname_list = potential_files.strip().split('\n')
        else:
            fname_list = potential_files.decode().strip().split('\n')

        for fname in fname_list:
            # print fname
            parts = fname.strip().split('.warts.gz')
            # print parts
            file_ctime = parts[0][-10:]
            # print file_ctime

            round_num = int(file_ctime)/600

            if int(round_num) == reqd_round_num:
                # We found a warts file that belongs to this round and needs to be processed

                if is_setup_done == 0:
                    op_log_fp, vp_to_vpnum_fp = setup_stuff()
                    is_setup_done = 1

                fname_suf = (fname.strip().split('/'))[-1]
                # sys.stderr.write("fname_suf: {0}\n".format(fname_suf) )

                vp = (fname_suf.strip().split('.'))[0]
                # sys.stderr.write("vp: {0}\n".format(vp) )

                # NOTE: If we ever decide to read the same VP's files from previous and this round, the vp_to_vpnum dict ensures we would still only use a single vpnum for a vp. Otherwise, the same VP could have one vpnum for the previous round and a different vpnum for the current round.
                if vp not in vp_to_vpnum:
                    vp_to_vpnum[vp] = vpnum
                    vp_to_vpnum_fp.write("{0} {1}\n".format(vp, vpnum) )
                    vpnum += 1

                # update_addr_to_resps(fname, addr_to_resps, vp_to_vpnum[vp])
                readwarts_update_addr_to_resps(fname, addr_to_resps, vp_to_vpnum[vp])

                op_log_fp.write("Done reading {0} at: {1}\n".format(fname, str(datetime.datetime.now() ) ) )

    # If there is anything in addr_to_resps, let's write it.
    write_addr_to_resps(addr_to_resps, processed_op_dir, round_tstart, round_tend, op_log_fp)

    op_log_fp.close()
    vp_to_vpnum_fp.close()


############## Read command line args and call main() #####################
campaign = sys.argv[1] # CO_VT_RI/FL/iran_addrs
round_tstart = int(sys.argv[2])
write_bin = int(sys.argv[3])

if write_bin == 1:
    pass

round_tend = round_tstart + 600
# NOTE: Change output dir for each test!
processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_testbin'.format(campaign)

main()
