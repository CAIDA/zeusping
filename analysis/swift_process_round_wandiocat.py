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
import subprocess
import os
import datetime
import json
from collections import defaultdict
import array
import io
import struct
import socket
import gmpy


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
    vp_to_resps_fp = open('{0}/{1}_to_{2}/vp_to_resps.log'.format(processed_op_dir, round_tstart, round_tend), 'w')

    return op_log_fp, vp_to_vpnum_fp, vp_to_resps_fp

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

    if swift_mode == 'swift': 

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

    else:

        inp_path = swift_mode
        pathh = '{0}/campaign={1}/year={2}/month={3}/day={4}/hour={5}/'.format(inp_path, campaign, round_tstart_dt.year, round_tstart_dt.strftime("%m"), round_tstart_dt.strftime("%d"), round_tstart_dt.strftime("%H"))

        potential_filenames = os.listdir(pathh)

        potential_files = []
        for fil in potential_filenames:
            abs_path = "{0}{1}".format(pathh, fil)
            potential_files.append(abs_path)

        potential_files.sort() # This is not strictly necessary. Only doing it for testing purposes.
            
    return potential_files

@profile        
def update_addr_to_resps(fname, addr_to_resps, vpnum, vp, vp_to_resps_fp):

    if swift_mode == 'swift':
        wandiocat_cmd = './swift_wrapper.sh swift://zeusping-warts/{0}'.format(fname)
    else:
        wandiocat_cmd = './swift_wrapper.sh {0}'.format(fname)
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

    # Keep track of response statistics for this VP
    vp_to_resps = array.array('L', [0, 0, 0, 0, 0])

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

            # try:
            #     dstipid = struct.unpack("!I", socket.inet_aton(dst))[0]
            # except socket.error:
            #     continue
            dstipid = zeusping_helpers.ipstr_to_ipint(dst)
            if dstipid == -1: # ipstr_to_ipint throws -1 on error
                continue
            
            # Don't increment. Bit shift to vp_num position and set bit to 1
            addr_to_resps[dstipid][0] |= (mask)  # 0th index is sent packets
            vp_to_resps[0] += 1

            # pinged_ts = data['start']['sec']

            resps = data['responses']

            if resps: # Apparently this way of checking for elements in a list is much faster than checking len
                this_resp = resps[0]
                icmp_type = this_resp["icmp_type"]
                icmp_code = this_resp["icmp_code"]

                if icmp_type == 0 and icmp_code == 0:
                    # Responded to the ping and response is indicative of working connectivity
                    addr_to_resps[dstipid][1] |= (mask) # 1st index is successful ping response
                    vp_to_resps[1] += 1
                elif icmp_type == 3 and icmp_code == 1:
                    # Destination host unreachable
                    addr_to_resps[dstipid][2] |= (mask) # 2nd index is Destination host unreachable
                    vp_to_resps[2] += 1
                else:
                    addr_to_resps[dstipid][3] |= (mask) # 3rd index is the rest of icmp stuff. So mostly errors.
                    vp_to_resps[3] += 1
            else:

                addr_to_resps[dstipid][4] |= (mask) # 4th index is lost ping
                vp_to_resps[4] += 1
            
    proc.wait() # Wait for the subprocess to exit

    # remaining_ping_lines = proc.communicate()[0]
    # for line in remaining_ping_lines.splitlines():
    #     line_ct += 1

    # Update vp_to_resps
    vp_to_resps_fp.write("{0} {1} {2} {3} {4} {5} {6}\n".format(vp, vpnum, vp_to_resps[0], vp_to_resps[1], vp_to_resps[2], vp_to_resps[3], vp_to_resps[4]) )


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
    

def write_resps_per_round_ascii(addr_to_resps, processed_op_dir, round_tstart, round_tend):
    op_fname = '{0}/{1}_to_{2}/resps_per_round_ascii'.format(processed_op_dir, round_tstart, round_tend)
    ping_aggrs_fp = open(op_fname, 'w')
    dst_ct = 0
    for dst in addr_to_resps:

        # Go from ipid to ipstr
        dstipstr = zeusping_helpers.ipint_to_ipstr(dst)

        # dst_ct += 1 # Since we don't use this variable, let's not update it. We get to save 6 seconds according to kernprof
        this_d = addr_to_resps[dst]
        ping_aggrs_fp.write("{0} {1} {2} {3} {4} {5}\n".format(dstipstr, this_d[0], this_d[1], this_d[2], this_d[3], this_d[4] ) )

    ping_aggrs_fp.close()

    return op_fname


def write_resps_per_round_bin(addr_to_resps, processed_op_dir, round_tstart, round_tend):
    op_fname = '{0}/{1}_to_{2}/resps_per_round'.format(processed_op_dir, round_tstart, round_tend)
    ping_aggrs_fp = open(op_fname, 'wb')

    if mode == 'annotate':
        # TODO: Check if we can initialize these dicts using defaultdicts somehow
        loc1_asn_to_status = {}
        loc2_asn_to_status = {}

    # Write to ping_aggrs_fp. Also calculate timeseries vals for various IODA aggregates.
    for dst in sorted(addr_to_resps):
        ping_aggrs_fp.write(struct_fmt.pack(dst, *(addr_to_resps[dst]) ) )

        # Go from ipid to ipstr
        dstipstr = zeusping_helpers.ipint_to_ipstr(dst)

        if mode == 'annotate':
            asn = 'UNK'
            # Find ip_to_as, ip_to_loc
            rnode = rtree.search_best(dstipstr)
            if rnode is None:
                asn = 'UNK'
            else:
                asn = rnode.data["origin"]

            # Let loc1 refer to the first-level location and loc2 refer to the second-level location
            # In the US, loc1 is state and loc2 is county
            # In non-US countries, loc1 will be country and loc2 will be region
            # At this point, we will obtain just the ids of loc1 and loc2. We will use other dictionaries to obtain the name and fqdn
            loc1 = 'UNKLOC1'
            loc2 = 'UNKLOC2'
            res = ipm.lookup(dstipstr)

            if len(res) != 0:
                ctry_code = res[0]['country_code']

                if is_US is True:
                    # Find US state and county
                    if ctry_code != 'US':
                        continue

                    loc1 = str(res[0]['polygon_ids'][1]) # This is for US state info
                    loc2 = str(res[0]['polygon_ids'][0]) # This is for county info

                else:
                    # Find region
                    loc1 = ctry_code
                    loc2 = str(res[0]['polygon_ids'][1]) # This is for region info

            this_arr = addr_to_resps[dst]
            # If this address received at least two pings, it was pinged
            if gmpy.popcount(this_arr[0]) >= 2:

                if loc1 not in loc1_asn_to_status:
                    loc1_asn_to_status[loc1] = {}

                if asn not in loc1_asn_to_status[loc1]:
                    loc1_asn_to_status[loc1][asn] = defaultdict(int)

                if loc2 not in loc2_asn_to_status:
                    loc2_asn_to_status[loc2] ={}

                if asn not in loc2_asn_to_status[loc2]:
                    loc2_asn_to_status[loc2][asn] = defaultdict(int)

                loc1_asn_to_status[loc1][asn]["pinged"] += 1
                loc2_asn_to_status[loc2][asn]["pinged"] += 1

            # If this address responded to at least one ping, it was responsive
            if this_arr[1] >= 1:

                if loc1 not in loc1_asn_to_status:
                    loc1_asn_to_status[loc1] = {}

                if asn not in loc1_asn_to_status[loc1]:
                    loc1_asn_to_status[loc1][asn] = defaultdict(int)

                if loc2 not in loc2_asn_to_status:
                    loc2_asn_to_status[loc2] ={}

                if asn not in loc2_asn_to_status[loc2]:
                    loc2_asn_to_status[loc2][asn] = defaultdict(int)

                loc1_asn_to_status[loc1][asn]["resp"] += 1
                loc2_asn_to_status[loc2][asn]["resp"] += 1

    ping_aggrs_fp.close()

    if mode == 'annotate':
        # Write ts file
        ts_fname = '{0}/{1}_to_{2}/ts'.format(processed_op_dir, round_tstart, round_tend)
        ts_fp = open(ts_fname, 'w')

        loc1_to_status = {}
        asn_to_status = {}

        for loc1 in loc1_asn_to_status:
            for asn in loc1_asn_to_status[loc1]:

                if is_US is True:
                    loc1_fqdn = idx_to_loc1_fqdn[loc1]
                    loc1_name = idx_to_loc1_name[loc1]
                else:
                    loc1_fqdn = ctry_code_to_fqdn[loc1]
                    loc1_name = ctry_code_to_name[loc1]

                ioda_key = 'projects.zeusping.test1.geo.netacuity.{0}.asn.{1}'.format(loc1_fqdn, asn)
                this_d = loc1_asn_to_status[loc1][asn]
                n_p = this_d["pinged"]
                n_r = this_d["resp"]
                custom_name = "{0}-{1}".format(loc1_name, asn)
                ts_fp.write("{0}|{1}|{2}|{3}\n".format(ioda_key, custom_name, n_p, n_r) )

                if loc1 not in loc1_to_status:
                    loc1_to_status[loc1] = {"pinged" : 0, "resp" : 0}

                loc1_to_status[loc1]["pinged"] += n_p
                loc1_to_status[loc1]["resp"] += n_r

                if asn not in asn_to_status:
                    asn_to_status[asn] = {"pinged" : 0, "resp" : 0}

                asn_to_status[asn]["pinged"] += n_p
                asn_to_status[asn]["resp"] += n_r

        for loc1 in loc1_to_status:
            if is_US is True:
                loc1_fqdn = idx_to_loc1_fqdn[loc1]
                loc1_name = idx_to_loc1_name[loc1]
            else:
                loc1_fqdn = ctry_code_to_fqdn[loc1]
                loc1_name = ctry_code_to_name[loc1]

            ioda_key = 'projects.zeusping.test1.geo.netacuity.{0}'.format(loc1_fqdn)
            this_d = loc1_to_status[loc1]
            n_p = this_d["pinged"]
            n_r = this_d["resp"]
            custom_name = "{0}".format(loc1_name)
            ts_fp.write("{0}|{1}|{2}|{3}\n".format(ioda_key, custom_name, n_p, n_r) )

        for asn in asn_to_status:

            ioda_key = 'projects.zeusping.test1.routing.asn.{0}'.format(asn)
            this_d = asn_to_status[asn]
            n_p = this_d["pinged"]
            n_r = this_d["resp"]
            custom_name = "{0}".format(asn)
            ts_fp.write("{0}|{1}|{2}|{3}\n".format(ioda_key, custom_name, n_p, n_r) )
    
        loc2_to_status = defaultdict(int) # TODO: We initialize loc2_to_status as a defaultdict(int), but use it as a defaultdict(defaultdict(int))! Fix this.

        for loc2 in loc2_asn_to_status:
            for asn in loc2_asn_to_status[loc2]:

                loc2_fqdn = idx_to_loc2_fqdn[loc2]
                loc2_name = idx_to_loc2_name[loc2]

                ioda_key = 'projects.zeusping.test1.geo.netacuity.{0}.asn.{1}'.format(loc2_fqdn, asn)
                this_d = loc2_asn_to_status[loc2][asn]
                n_p = this_d["pinged"]
                n_r = this_d["resp"]
                custom_name = "{0}-{1}".format(loc2_name, asn)
                ts_fp.write("{0}|{1}|{2}|{3}\n".format(ioda_key, custom_name, n_p, n_r) )

                if loc2 not in loc2_to_status:
                    loc2_to_status[loc2] = {"pinged" : 0, "resp" : 0}

                loc2_to_status[loc2]["pinged"] += n_p
                loc2_to_status[loc2]["resp"] += n_r

        for loc2 in loc2_to_status:
            loc2_fqdn = idx_to_loc2_fqdn[loc2]
            loc2_name = idx_to_loc2_name[loc2]

            ioda_key = 'projects.zeusping.test1.geo.netacuity.{0}'.format(loc2_fqdn)
            this_d = loc2_to_status[loc2]
            n_p = this_d["pinged"]
            n_r = this_d["resp"]
            custom_name = "{0}".format(loc2_name)
            ts_fp.write("{0}|{1}|{2}|{3}\n".format(ioda_key, custom_name, n_p, n_r) )

    if mode == 'annotate':
        return op_fname, ts_fname
    else:
        return op_fname
    
@profile
def write_addr_to_resps(addr_to_resps, processed_op_dir, round_tstart, round_tend, op_log_fp):
    if len(addr_to_resps) > 0:

        if write_bin == 0:
            op_fname = write_resps_per_round_ascii(addr_to_resps, processed_op_dir, round_tstart, round_tend)
        else:
            if mode == 'annotate':
                op_fname, ts_fname = write_resps_per_round_bin(addr_to_resps, processed_op_dir, round_tstart, round_tend)
                
                # Compress the ts file
                gzip_cmd = 'gzip {0}'.format(ts_fname)
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

                # TODO: Perhaps cp ts.gz to another temporary directory? We will keep the ts.gz files around in /scratch for a long time since they are really small and these are the files that we may like to access frequently?

            else:
                op_fname = write_resps_per_round_bin(addr_to_resps, processed_op_dir, round_tstart, round_tend)

        # Compress the output file
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
            if swift_mode == 'swift':
                fname_list = potential_files.strip().split('\n')
            else:
                fname_list = potential_files
        else:
            if swift_mode == 'swift':
                fname_list = potential_files.decode().strip().split('\n')
            else:
                fname_list = potential_files

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
                    op_log_fp, vp_to_vpnum_fp, vp_to_resps_fp = setup_stuff()
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

                update_addr_to_resps(fname, addr_to_resps, vp_to_vpnum[vp], vp, vp_to_resps_fp)
                # readwarts_update_addr_to_resps(fname, addr_to_resps, vp_to_vpnum[vp]) # Unfortunately, this function was woefully slow, so reverting to reading the output of sc_warts2json

                op_log_fp.write("Done reading {0} at: {1}\n".format(fname, str(datetime.datetime.now() ) ) )

    # If there is anything in addr_to_resps, let's write it.
    write_addr_to_resps(addr_to_resps, processed_op_dir, round_tstart, round_tend, op_log_fp)

    op_log_fp.close()
    vp_to_vpnum_fp.close()
    vp_to_resps_fp.close()

    # NOTE: Some other process will find all these files, upload them to Swift, and delete temporary files. This process is doing enough as it is and it needs to execute in near-real time. So let's move the uploading to Swift and deletion of temporary files to a separate (and independent) process


############## Read command line args and call main() #####################
campaign = sys.argv[1] # CO_VT_RI/FL/iran_addrs
round_tstart = int(sys.argv[2])
write_bin = int(sys.argv[3])
mode = sys.argv[4]
swift_mode = sys.argv[5] # swift_mode == 'swift' indicates that the files reside on the Swift cluster. If this argument contains a path instead, it indicates the path on local disk where the files reside.
processed_op_dir = sys.argv[6]
# NOTE: Change output dir for each test!
# processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_testbin'.format(campaign)


if write_bin == 1:
    struct_fmt = struct.Struct("I 5H")

if mode == 'annotate':

    import radix
    import pyipmeta
    
    pfx2AS_fn = sys.argv[7]
    netacq_date = sys.argv[8]
    scope = sys.argv[9]

    idx_to_loc1_name = {}
    idx_to_loc1_fqdn = {}
    idx_to_loc1_code = {}

    idx_to_loc2_name = {}
    idx_to_loc2_fqdn = {}
    idx_to_loc2_code = {}

    if scope == 'US':
        is_US = True
        # loc1 is regions, loc2 is counties
        regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
        zeusping_helpers.load_idx_to_dicts(regions_fname, idx_to_loc1_fqdn, idx_to_loc1_name, idx_to_loc1_code, py_ver=py_ver)
        counties_fname = '/data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz'
        zeusping_helpers.load_idx_to_dicts(counties_fname, idx_to_loc2_fqdn, idx_to_loc2_name, idx_to_loc2_code, py_ver=py_ver)

    else:
        is_US = False
        # loc1 is countries, loc2 is regions

        ctry_code_to_fqdn = {}
        ctry_code_to_name = {}
        countries_fname = '/data/external/natural-earth/polygons/ne_10m_admin_0.countries.v3.1.0.processed.polygons.csv.gz'
        zeusping_helpers.load_idx_to_dicts(countries_fname, idx_to_loc1_fqdn, idx_to_loc1_name, idx_to_loc1_code, ctry_code_to_fqdn=ctry_code_to_fqdn, ctry_code_to_name=ctry_code_to_name, py_ver=py_ver)

        regions_fname = '/data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz'
        zeusping_helpers.load_idx_to_dicts(regions_fname, idx_to_loc2_fqdn, idx_to_loc2_name, idx_to_loc2_code, py_ver=py_ver)

    rtree = radix.Radix()
    zeusping_helpers.load_radix_tree(pfx2AS_fn, rtree, py_ver=py_ver)

    # Load pyipmeta in order to perform geo lookups per address
    provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
    ipm = pyipmeta.IpMeta(provider="netacq-edge",
                          provider_config=provider_config_str)

round_tend = round_tstart + 600

main()
