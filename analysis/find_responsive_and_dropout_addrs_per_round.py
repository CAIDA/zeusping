#!/usr/bin/env python3

#  This software is Copyright (c) 2019 The Regents of the University of
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

# This script uses processed ping responses from round t-1 and round t to calculate the number of dropouts, responsive, and antidropout addresses per round
# Conceptually, at the beginning of a round t, there are N (say 1000) addresses that responded in round t-1 and can potentially dropout this round. Let's suppose there are D dropouts (say 100), then 900 of them continued to respond in this round as well (NOTE: the way erosprober works, if an address hasn't dropped out, then it *must* be responsive since all addresses are pinged each round). However, it is possible that some addresses dropped out due to reassignment so M *other* addresses (say 50) lit up. Then the total number of outages is D - M.

# We took up about 10G of memory (now seem to be taking up ~6.5G of memory). We previously required only 1G of memory.

import sys
import os
import datetime
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
import radix
import pyipmeta
    
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
def split_ips(ip):
    parts = ip.strip().split('.')
    # print parts
    oct1 = parts[0].strip()
    oct2 = parts[1].strip()
    oct3 = parts[2].strip()
    oct4 = parts[3].strip()

    return oct1, oct2, oct3, oct4

@profile
def find_s24(ipv4_addr):
    oct1, oct2, oct3, oct4 = split_ips(ipv4_addr)
    return "{0}.{1}.{2}.0/24".format(oct1, oct2, oct3), oct4

@profile
def setup_stuff(processed_op_dir, this_t, this_t_round_end):
    mkdir_cmd = 'mkdir -p {0}/{1}_to_{2}/'.format(processed_op_dir, this_t, this_t_round_end)
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

@profile         
def get_fp(processed_op_dir, reqd_t, read_bin, is_swift, called_from):

    if read_bin == 1:

        if is_swift == 1:
            this_t_dt = datetime.datetime.utcfromtimestamp(reqd_t)
            round_id = "{0}_to_{1}".format(reqd_t, reqd_t + zeusping_helpers.ROUND_SECS)
            reqd_t_file = 'datasource=zeusping/campaign={0}/year={1}/month={2}/day={3}/hour={4}/round={5}/resps_per_round.gz'.format(campaign, this_t_dt.year, this_t_dt.strftime("%m"), this_t_dt.strftime("%d"), this_t_dt.strftime("%H"), round_id)
            wandiocat_cmd = 'wandiocat swift://zeusping-processed/{0}'.format(reqd_t_file)
        else:
            reqd_t_file = '{0}/{1}_to_{2}/resps_per_round.gz'.format(processed_op_dir, reqd_t, reqd_t + zeusping_helpers.ROUND_SECS)
            # reqd_t_fp = wandio.open(reqd_t_file, 'rb')
            wandiocat_cmd = 'wandiocat {0}'.format(reqd_t_file)

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

        sys.stderr.write("{0}: {1}\n".format(called_from, reqd_t_file) )

        return proc.stdout
            
    else:

        # NOTE: We could choose to implement the non-binary case with wandiocat as well but I'm choosing not to.
        if IS_COMPRESSED == 1:
            reqd_t_file = '{0}/{1}_to_{2}/resps_per_addr.gz'.format(processed_op_dir, reqd_t, reqd_t + zeusping_helpers.ROUND_SECS)
            reqd_t_fp = wandio.open(reqd_t_file, 'r')
        else:
            reqd_t_file = '{0}/{1}_to_{2}/resps_per_addr'.format(processed_op_dir, reqd_t + zeusping_helpers.ROUND_SECS)
            reqd_t_fp = open(reqd_t_file, 'r')
            
        sys.stderr.write("{0}: {1}\n".format(called_from, reqd_t_file) )
        return reqd_t_fp

@profile            
def get_resp_unresp_prev_round(processed_op_dir, this_t_func, read_bin, is_swift, unresp_addrs, resp_addrs):

    prev_t = this_t_func - zeusping_helpers.ROUND_SECS

    prev_t_fp = get_fp(processed_op_dir, prev_t, read_bin, is_swift, "Prev")

    done_ct = 0
    
    while True:
        
    # for line in prev_t_fp:

        if read_bin == 1:
            data_chunk = prev_t_fp.read(struct_fmt.size * 2000)

            if len(data_chunk) == 0:
                break

            gen = struct_fmt.iter_unpack(data_chunk)

            for elem in gen:

                done_ct += 1
                
                ipid, sent_pkts, successful_resps, host_unreach, icmp_err, losses = elem
                # ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

                # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent_pkts, successful_resps, host_unreach, icmp_err, losses) )
                # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, gmpy.popcount(sent_pkts), gmpy.popcount(successful_resps), gmpy.popcount(host_unreach), gmpy.popcount(icmp_err), gmpy.popcount(losses) ) )

                # if done_ct == 100:
                #     print(resp_addrs)
                #     print(unresp_addrs)
                #     sys.exit(1)

                # if ipstr == '67.181.30.249':
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent_pkts, successful_resps, host_unreach, icmp_err, losses) )
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, gmpy.popcount(sent_pkts), gmpy.popcount(successful_resps), gmpy.popcount(host_unreach), gmpy.popcount(icmp_err), gmpy.popcount(losses) ) )
                    # sys.exit(1)
                
                if gmpy.popcount(successful_resps) > 0:
                    # resp_addrs.add(ipstr)
                    resp_addrs.add(ipid)
                # NOTE: In future, let's consider special cases where there may only have been a single active vantage point. 
                elif ( (gmpy.popcount(losses) == gmpy.popcount(sent_pkts) ) and (gmpy.popcount(successful_resps) == 0) ):
                    # unresp_addrs.add(ipstr)
                    unresp_addrs.add(ipid)

            data_chunk = ''

        else:
            line = prev_t_fp.readline()

            if not line:
                break
        
            parts = line.strip().split()
            
            ipstr = parts[0]
            ipid = struct.unpack("!I", socket.inet_aton(ipstr))[0]

            sent_pkts = int(parts[1])
            successful_resps = int(parts[2])
            host_unreach = int(parts[3])
            icmp_err = int(parts[4])
            losses = int(parts[5])

            # TODO: Think about whether I should use host_unreach and icmp_err messages in some way
            if successful_resps > 0:
                resp_addrs.add(ipid) # This address was responsive in the previous round and has the potential to fail
            elif losses == sent_pkts:
                unresp_addrs.add(ipid) # This address was completely unresponsive in the previous round

    prev_t_fp.close()

@profile
def get_dropout_antidropout_resp_unresp(processed_op_dir, this_t_func, read_bin, is_swift, unresp_addrs, resp_addrs, addr_to_status, resp_addrs_this_round, unresp_addrs_this_round=None):

    # NOTE: unresp_addrs in this function refers to unresp_addrs from the previous round. unresp_addrs_this_round refers to unresp_addrs in this round (same logic applies to resp_addrs and resp_addrs_this_round)

    this_t_fp = get_fp(processed_op_dir, this_t_func, read_bin, is_swift, "This")

    if read_bin == 1:

        while True:
        
            data_chunk = this_t_fp.read(struct_fmt.size * 2000)

            if len(data_chunk) == 0:
                break

            gen = struct_fmt.iter_unpack(data_chunk)

            for elem in gen:

                ipid, sent_pkts, successful_resps, host_unreach, icmp_err, losses = elem
                # ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

                # if ipstr == '67.181.30.249':
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent_pkts, successful_resps, host_unreach, icmp_err, losses) )
                #     sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, gmpy.popcount(sent_pkts), gmpy.popcount(successful_resps), gmpy.popcount(host_unreach), gmpy.popcount(icmp_err), gmpy.popcount(losses) ) )
                #     sys.exit(1)

                # NOTE: The (gmpy.popcount(successful_resps) == 0) test is for unusual cases where a vantage point may have pinged an address more than once in a round and one of the responses was a loss but the other was successful. For a dropout, we want *all* pings to be unresponsive in this round.
                # NOTE: There is an even more unusual case where: an address was pinged more than once from a vantage point. One of the responses was a loss, satisying (gmpy.popcount(losses) == gmpy.popcount(sent_pkts). The other response was not a successful response but an icmp error. In this (edge) case, I am going to consider the address as having experienced a dropout because it still saw a lost ping from all vantage points.
                if ( (gmpy.popcount(losses) == gmpy.popcount(sent_pkts) ) and (ipid in resp_addrs) and (gmpy.popcount(successful_resps) == 0) ):
                    # dropout_addrs.add(ipstr)
                    addr_to_status[ipid] = 0
                    # dropout_addrs.add(ipid)

                elif ( (gmpy.popcount(successful_resps) > 0) and (ipid in unresp_addrs) ):
                    # antidropout_addrs.add(ipstr)
                    addr_to_status[ipid] = 2
                    # antidropout_addrs.add(ipid)

                if gmpy.popcount(successful_resps) > 0:
                    resp_addrs_this_round.add(ipid)
                elif unresp_addrs_this_round is not None:
                    if( (gmpy.popcount(losses) == gmpy.popcount(sent_pkts) ) and (gmpy.popcount(successful_resps) == 0) ):
                        unresp_addrs_this_round.add(ipid)
                        
            data_chunk = ''
            
    else:
        
        for line in this_t_fp:
            parts = line.strip().split()

            ipstr = parts[0]
            ipid = struct.unpack("!I", socket.inet_aton(ipstr))[0]

            sent_pkts = int(parts[1])
            successful_resps = int(parts[2])
            host_unreach = int(parts[3])
            icmp_err = int(parts[4])
            losses = int(parts[5])

            # If all pings sent in this round were lost and this address was responsive at the beginning of this round, then this address experienced a dropout.
            if losses == sent_pkts and ipid in resp_addrs:
                addr_to_status[ipid] = 0
                # dropout_addrs.add(ipstr)

            # If the address had been completely unresponsive last round but is responsive now, then this is a newly responsive address.
            elif successful_resps > 0 and ipid in unresp_addrs:
                addr_to_status[ipid] = 2
                # antidropout_addrs.add(ipstr)

            if resp_addrs_this_round != None:
                if successful_resps > 0:
                    resp_addrs_this_round.add(ipid)
                elif unresp_addrs_this_round is not None:
                    if losses == sent_pkts:
                        unresp_addrs_this_round.add(ipid)

    this_t_fp.close()

@profile
def init_dicts(loc1_asn_to_status, loc2_asn_to_status, loc1, loc2, asn):
    if loc1 not in loc1_asn_to_status:
        loc1_asn_to_status[loc1] = {}

    if asn not in loc1_asn_to_status[loc1]:
        loc1_asn_to_status[loc1][asn] = defaultdict(int)

    if loc2 not in loc2_asn_to_status:
        loc2_asn_to_status[loc2] = {}

    if asn not in loc2_asn_to_status[loc2]:
        loc2_asn_to_status[loc2][asn] = defaultdict(int)


# is_loc1 indicates whether we are writing the larger administrative subdivision
# if is_loc1 is True, then the larger administrative subdivision is country (if is_US == False) and U.S. state (if is_US == True).
@profile
def write_ts_file(ts_fp, loc_asn_to_status, is_loc1):

    loc_to_status = {}
    asn_to_status = {}
    
    for loc in loc_asn_to_status:
        for asn in loc_asn_to_status[loc]:

            if is_loc1 is True:
                if is_US is True:
                    loc_fqdn = idx_to_loc1_fqdn[loc]
                    loc_name = idx_to_loc1_name[loc]
                else:
                    loc_fqdn = ctry_code_to_fqdn[loc]
                    loc_name = ctry_code_to_name[loc]
            else:
                loc_fqdn = idx_to_loc2_fqdn[loc]
                loc_name = idx_to_loc2_name[loc]
                
            ioda_key = 'projects.zeusping.test1.geo.netacuity.{0}.asn.{1}'.format(loc_fqdn, asn)
            this_d = loc_asn_to_status[loc][asn]
            n_d = this_d["d"]
            n_r = this_d["r"]
            n_a = this_d["a"]
            custom_name = "{0}-{1}".format(loc_name, asn)
            ts_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(ioda_key, custom_name, n_d, n_r, n_a) )

            if loc not in loc_to_status:
                loc_to_status[loc] = {"d" : 0, "r" : 0, "a" : 0}

            loc_to_status[loc]["d"] += n_d
            loc_to_status[loc]["r"] += n_r
            loc_to_status[loc]["a"] += n_a

            # Calculate stats per ASN only for loc1 and not for loc2 (since the results would be identical in any case)
            if is_loc1 is True:
                if asn not in asn_to_status:
                    asn_to_status[asn] = {"d" : 0, "r" : 0, "a" : 0}

                asn_to_status[asn]["d"] += n_d
                asn_to_status[asn]["r"] += n_r
                asn_to_status[asn]["a"] += n_a

    for loc in loc_to_status:
        
        if is_loc1 is True:
            if is_US is True:
                loc_fqdn = idx_to_loc1_fqdn[loc]
                loc_name = idx_to_loc1_name[loc]
            else:
                loc_fqdn = ctry_code_to_fqdn[loc]
                loc_name = ctry_code_to_name[loc]
        else:
            loc_fqdn = idx_to_loc2_fqdn[loc]
            loc_name = idx_to_loc2_name[loc]

        ioda_key = 'projects.zeusping.test1.geo.netacuity.{0}'.format(loc_fqdn)
        this_d = loc_to_status[loc]

        n_d = this_d["d"]
        n_r = this_d["r"]
        n_a = this_d["a"]
        custom_name = "{0}".format(loc_name)
        ts_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(ioda_key, custom_name, n_d, n_r, n_a) )

    if is_loc1 is True:
        # TODO: Should I only perhaps write when is_US is False? Won't I be clobbering values in the U.S., when I write into AS7922 from multiple ZeusPing shards...?
        for asn in asn_to_status:
            ioda_key = 'projects.zeusping.test1.routing.asn.{0}'.format(asn)
            this_d = asn_to_status[asn]
            n_d = this_d["d"]
            n_r = this_d["r"]
            n_a = this_d["a"]

            custom_name = "{0}".format(asn)
            ts_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(ioda_key, custom_name, n_d, n_r, n_a) )
            
@profile     
def write_op(processed_op_dir, this_t_func, this_t_round_end, addr_to_status, resp_addrs):

    loc1_asn_to_status = defaultdict(nested_dict_factory)
    loc2_asn_to_status = defaultdict(nested_dict_factory)

    if IS_COMPRESSED == 1:
        if MUST_WRITE_RDA == 1:
            if IS_TEST == 1:            
                rda_op_fname = '{0}/{1}_to_{2}/rda_test.gz'.format(processed_op_dir, this_t_func, this_t_round_end)
            else:
                rda_op_fname = '{0}/{1}_to_{2}/rda.gz'.format(processed_op_dir, this_t_func, this_t_round_end)
            rda_op_fp = wandio.open(rda_op_fname, 'w')
    else:
        if IS_TEST == 1:
            rda_op_fname = '{0}/{1}_to_{2}/rda_test'.format(processed_op_dir, this_t_func, this_t_round_end)
        else:
            rda_op_fname = '{0}/{1}_to_{2}/rda'.format(processed_op_dir, this_t_func, this_t_round_end)
        rda_op_fp = open(rda_op_fname, 'w')

    this_roun_addr_to_status = addr_to_status[0]

    to_write_addr_set = this_roun_addr_to_status.keys() | resp_addrs[-1]

    # ip_to_asn = {}
    ip_to_loc = {}
    # ipint_to_ipstr = {}

    s24_to_sr_status = defaultdict(nested_dict_factory_int)
    
    for addr in sorted(to_write_addr_set):
        
        # if read_bin == 1:
        ipstr = socket.inet_ntoa(struct.pack('!L', addr))
        # ipint_to_ipstr[addr] = ipstr
        # else:
        #     ipstr = addr

        # Find ASN and loc details
        # asn = 'UNK' # TODO: This is a redundant operation, needs to go.
        # Find ip_to_as, ip_to_loc
        rnode = rtree.search_best(ipstr)
        if rnode is None:
            asn = 'UNK'
        else:
            asn = rnode.data["origin"]
        # ip_to_asn[ipstr] = asn

        # Let loc1 refer to the first-level location and loc2 refer to the second-level location
        # In the US, loc1 is state and loc2 is county
        # In non-US countries, loc1 will be country and loc2 will be region
        # At this point, we will obtain just the ids of loc1 and loc2. We will use other dictionaries to obtain the name and fqdn
        loc1 = 'UNKLOC1'
        loc2 = 'UNKLOC2'
        res = ipm.lookup(ipstr)

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
                
        ip_to_loc[ipstr] = [loc1, loc2]

        # s24, oct4 = find_s24(ipstr) # TODO: We need a faster way of identifying an address's /24, this uses a lot of string functions that are slow.
        s24 = addr & s24_mask
        oct4 = addr & oct4_mask
        
        if addr in this_roun_addr_to_status:
            if this_roun_addr_to_status[addr] == 0:

                loc1_asn_to_status[loc1][asn]["r"] += 1
                loc2_asn_to_status[loc2][asn]["r"] += 1
                if MUST_WRITE_RDA == 1:
                    rda_op_fp.write("{0} 1\n".format(ipstr) ) # The address was responsive at the beginning of the round
                mask = bitset_cache[oct4]
                s24_to_sr_status[s24]['r'] |= (mask)
                
                loc1_asn_to_status[loc1][asn]["d"] += 1
                loc2_asn_to_status[loc2][asn]["d"] += 1
                s24_to_sr_status[s24]['d'] |= (mask)
                if MUST_WRITE_RDA == 1:
                    rda_op_fp.write("{0} 0\n".format(ipstr) ) # The address experienced a dropout this round
                
            else:
                # if addr_to_status is not 0, it *has* to be 2
                loc1_asn_to_status[loc1][asn]["a"] += 1
                loc2_asn_to_status[loc2][asn]["a"] += 1
                mask = bitset_cache[oct4]
                s24_to_sr_status[s24]['a'] |= (mask)
                if MUST_WRITE_RDA == 1:
                    rda_op_fp.write("{0} 2\n".format(ipstr) ) # The address experienced an anti-dropout this round

        else:
            loc1_asn_to_status[loc1][asn]["r"] += 1
            loc2_asn_to_status[loc2][asn]["r"] += 1
            mask = bitset_cache[oct4]
            s24_to_sr_status[s24]['r'] |= (mask)
            if MUST_WRITE_RDA == 1:
                rda_op_fp.write("{0} 1\n".format(ipstr) ) # The address was responsive at the beginning of the round

    if MUST_WRITE_RDA == 1:                
        rda_op_fp.close() # wandio does not like it if the fp is not closed explicitly

    if IS_TEST == 1:
        ts_fname = '{0}/{1}_to_{2}/ts_rda_test'.format(processed_op_dir, this_t_func, this_t_round_end)
    else:
        ts_fname = '{0}/{1}_to_{2}/ts_rda'.format(processed_op_dir, this_t_func, this_t_round_end)
    ts_fp = open(ts_fname, 'w')
    write_ts_file(ts_fp, loc1_asn_to_status, True)
    write_ts_file(ts_fp, loc2_asn_to_status, False)
    ts_fp.close()

    if IS_TEST == 1:
        s24_fname = '{0}/{1}_to_{2}/ts_s24_sr_test'.format(processed_op_dir, this_t_func, this_t_round_end)
    else:
        s24_fname = '{0}/{1}_to_{2}/ts_s24_sr'.format(processed_op_dir, this_t_func, this_t_round_end)
    s24_fp = open(s24_fname, 'w')

    for s24 in s24_to_sr_status:
        this_d = s24_to_sr_status[s24]
        s24_fp.write("{0}|{1}|{2}|{3}\n".format(s24, this_d['d'], this_d['r'], this_d['a']) )
    
    s24_fp.close()

    s24_to_sr_status = {} # Release memory
    # Reinitialize loc1_asn_to_status and loc2_asn_to_status for mr since we no longer need the sr versions. Hopefully, this frees the memory.
    loc1_asn_to_status = defaultdict(nested_dict_factory)
    loc2_asn_to_status = defaultdict(nested_dict_factory)

    if MUST_WRITE_RDA_MR == 1:

        if IS_COMPRESSED == 1:
            if IS_TEST == 1:
                rda_multiround_op_fname = '{0}/{1}_to_{2}/rda_multiround_test.gz'.format(processed_op_dir, this_t_func, this_t_round_end)
            else:
                rda_multiround_op_fname = '{0}/{1}_to_{2}/rda_multiround.gz'.format(processed_op_dir, this_t_func, this_t_round_end)
            rda_multiround_op_fp = wandio.open(rda_multiround_op_fname, 'w')
        else:
            if IS_TEST == 1:
                rda_multiround_op_fname = '{0}/{1}_to_{2}/rda_multiround_test'.format(processed_op_dir, this_t_func, this_t_round_end)
            else:
                rda_multiround_op_fname = '{0}/{1}_to_{2}/rda_multiround'.format(processed_op_dir, this_t_func, this_t_round_end)
            rda_multiround_op_fp = open(rda_multiround_op_fname, 'w')

    # TODO: Replace addr_to_status[-1] with prev_round_addr_to_status etc., to prevent repeated hashing
    # resp_all_rounds_addr_set = resp_addrs[-1] & resp_addrs[0] & resp_addrs[1]
    resp_all_rounds_addr_set = resp_addrs[-2] & resp_addrs[-1] & resp_addrs[0] & resp_addrs[1]
    # del resp_addrs
    # sys.stderr.write("Deleted resp_addrs\n") # Deleting did not seem to make any difference!
    # to_write_addr_set = addr_to_status[-1].keys() | this_roun_addr_to_status.keys() | addr_to_status[1].keys() | resp_all_rounds_addr_set
    to_write_addr_set = addr_to_status[-1].keys() | this_roun_addr_to_status.keys() | resp_all_rounds_addr_set

    # addr_to_multiround_status = {}
    s24_to_mr_status = defaultdict(nested_dict_factory_int)
    for addr in sorted(to_write_addr_set):
        
        # if read_bin == 1:
        # Tried caching ipint_to_ipstr; we got very little benefit for non-trivial added memory consumption. Undid the caching.
        # if addr in ipint_to_ipstr:
        #     ipstr = ipint_to_ipstr[addr]
        # else:
        ipstr = socket.inet_ntoa(struct.pack('!L', addr))
        # else:
        #     ipstr = addr

        if MUST_WRITE_RDA_MR == 1:
            # Not all the mr IP addresses will be in the sr IP addresses.
            # But for those that are, let's find loc details using the dict, to save processing time.

            # Caching ip_to_asn was not helping significantly with speed, so decided to not use the cache.
            asn = 'UNK'
            # Find ip_to_as, ip_to_loc
            rnode = rtree.search_best(ipstr)
            if rnode is None:
                asn = 'UNK'
            else:
                asn = rnode.data["origin"]
            
            if ipstr in ip_to_loc:
                # asn = ip_to_asn[ipstr]
                [loc1, loc2] = ip_to_loc[ipstr]

            else:

                loc1 = 'UNKLOC1'
                loc2 = 'UNKLOC2'
                res = ipm.lookup(ipstr)

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
            
        # s24, oct4 = find_s24(ipstr) # TODO: We need a faster way of identifying an address's /24, this uses a lot of string functions that are slow.
        s24 = addr & s24_mask
        oct4 = addr & oct4_mask

        # if( ( addr_to_status[-1][addr] == 0) or (addr_to_status[0][addr]== 0) or (addr_to_status[1][addr] == 0) ):
        if( ( addr_to_status[-1][addr] == 0) or (addr_to_status[0][addr]== 0) ):
            if MUST_WRITE_RDA_MR == 1:
                # addr_to_multiround_status[addr] = 0
                loc1_asn_to_status[loc1][asn]["d"] += 1
                loc2_asn_to_status[loc2][asn]["d"] += 1
                rda_multiround_op_fp.write("{0} 0\n".format(ipstr) )
                
            mask = bitset_cache[oct4]
            s24_to_mr_status[s24]['d'] |= (mask)
            
        elif addr in resp_all_rounds_addr_set:
            if MUST_WRITE_RDA_MR == 1:
                # addr_to_multiround_status[addr] = 1
                loc1_asn_to_status[loc1][asn]["r"] += 1
                loc2_asn_to_status[loc2][asn]["r"] += 1
                rda_multiround_op_fp.write("{0} 1\n".format(ipstr) )
                
            mask = bitset_cache[oct4]
            s24_to_mr_status[s24]['r'] |= (mask)

        # if ( ( addr_to_status[-1][addr] == 2) or (addr_to_status[0][addr] == 2) or (addr_to_status[1][addr] == 2) ):
        if ( ( addr_to_status[-1][addr] == 2) or (addr_to_status[0][addr] == 2) ):
            if MUST_WRITE_RDA_MR == 1:
                # addr_to_multiround_status[addr] = 2
                loc1_asn_to_status[loc1][asn]["a"] += 1
                loc2_asn_to_status[loc2][asn]["a"] += 1
                rda_multiround_op_fp.write("{0} 2\n".format(ipstr) )
                
            mask = bitset_cache[oct4]
            s24_to_mr_status[s24]['a'] |= (mask)
            
        # rda_multiround_op_fp.write("{0} {1}\n".format(ipstr, addr_to_multiround_status[addr]) )
        
    if MUST_WRITE_RDA_MR == 1:
        rda_multiround_op_fp.close() # wandio does not like it if the fp is not closed explicitly

        if IS_TEST == 1:
            ts_fname = '{0}/{1}_to_{2}/ts_rda_mr_test'.format(processed_op_dir, this_t_func, this_t_round_end)
        else:
            ts_fname = '{0}/{1}_to_{2}/ts_rda_mr'.format(processed_op_dir, this_t_func, this_t_round_end)
        ts_fp = open(ts_fname, 'w')
        write_ts_file(ts_fp, loc1_asn_to_status, True)
        write_ts_file(ts_fp, loc2_asn_to_status, False)
        ts_fp.close()

    if IS_TEST == 1:
        s24_fname = '{0}/{1}_to_{2}/ts_s24_mr_test'.format(processed_op_dir, this_t_func, this_t_round_end)
    else:
        s24_fname = '{0}/{1}_to_{2}/ts_s24_mr'.format(processed_op_dir, this_t_func, this_t_round_end)
    s24_fp = open(s24_fname, 'w')

    for s24 in s24_to_mr_status:
        this_d = s24_to_mr_status[s24]
        s24_fp.write("{0}|{1}|{2}|{3}\n".format(s24, this_d['d'], this_d['r'], this_d['a']) )
    
    s24_fp.close()
            
@profile
def main():
    
    setup_stuff(processed_op_dir, this_t, this_t_round_end)

    # These are the set of responsive addresses at the beginning of a round (i.e., they were responsive in the previous round).
    # For multi-round analyses, we will need resp_addrs from multiple rounds to identify addresses that had remained responsive, even when other addresses in the /24 had dropped out.
    # Thus, maintain resp_addrs in a dict (each round is a key, value is a set of resp_addrs)
    resp_addrs = {}

    # These are the set of unresponsive addresses at the beginning of a round (i.e., they were unresponsive in the previous round)
    # NOTE: We only need unresp_addrs from a single previous round. This is because unresp_addrs across multiple rounds are not necessary for multi-round analyses
    # so we do not need to maintain unresp_addrs per round in a dict
    # unresp_addrs = {} 

    # addr_to_status contains information on whether an address had a dropout/antidropout
    addr_to_status = {}

    for roun in range(-num_adjacent_rounds, (num_adjacent_rounds+1) ):

        # Get unresponsive and responsive addresses using the previous round.
        # responsive addresses are all the addresses that have the potential to dropout in this round.
        # unresponsive addresses are all the addresses that have the potential to anti-dropout in this round.

        if roun-1 not in resp_addrs:
            resp_addrs[roun-1] = set()

        # For the very first iteration, we need to get resp_addrs and unresp_addrs using a separate function
        if roun == -num_adjacent_rounds:
            unresp_addrs_prev_round = set()
            get_resp_unresp_prev_round(processed_op_dir, this_t + roun * zeusping_helpers.ROUND_SECS, read_bin, is_swift, unresp_addrs_prev_round, resp_addrs[roun-1])

        # print len(unresp_addrs)

        # Get dropout and antidropout addresses using this round.
        # Also get responsive and unresponsive addresses from this round; they will be used for calculating dropouts and antidropouts the next round.
        # We don't need unresponsive addresses for the last round, however.
        
        # dropout addresses are all the addresses that responded last round but are unresponsive this round.
        # antidropout addresses are all the addresses that were unresponsive last round but responded this round.

        if roun not in addr_to_status:
            addr_to_status[roun] = defaultdict(lambda:9999)

        if roun < num_adjacent_rounds:
            resp_addrs[roun] = set()
            unresp_addrs_this_round = set()

        else:
            # Do not get unresp_addrs_this_round for the last round. But we still need resp_addrs from the last round to identify which addresses remained responsive across multiple rounds (including the last round).            
            resp_addrs[roun] = set()
            unresp_addrs_this_round = None
            
        get_dropout_antidropout_resp_unresp(processed_op_dir, this_t + roun * zeusping_helpers.ROUND_SECS, read_bin, is_swift, unresp_addrs_prev_round, resp_addrs[roun-1], addr_to_status[roun], resp_addrs[roun], unresp_addrs_this_round)

        # Update unresp_addrs for next round
        unresp_addrs_prev_round = unresp_addrs_this_round

    # We're done with unresp_addrs, free memory
    # TODO: Experiment with the following
    # del unresp_addrs_prev_round
    # gc.collect()

    # sys.exit(1) # Around 5 GB of memory taken up at this point
    
    write_op(processed_op_dir, this_t, this_t_round_end, addr_to_status, resp_addrs)

    
campaign = sys.argv[1]
this_t = int(sys.argv[2])
read_bin = int(sys.argv[3]) # If read_bin == 1, we are reading binary input from resps_per_round.gz. Else we are reading ascii input from resps_per_addr.gz
is_swift = int(sys.argv[4]) # Whether we are reading input files from the Swift cluster or from disk

pfx2AS_fn = sys.argv[5]
netacq_date = sys.argv[6]
scope = sys.argv[7]

MUST_WRITE_RDA = 1
MUST_WRITE_RDA_MR = 1

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
rnode = zeusping_helpers.load_radix_tree(pfx2AS_fn, rtree, py_ver=3)

# Load pyipmeta in order to perform geo lookups per address
provider_config_str = "-b /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-blocks.csv.gz -l /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-locations.csv.gz -p /data/external/netacuity-dumps/Edge-processed/{0}.netacq-4-polygons.csv.gz -t /data/external/gadm/polygons/gadm.counties.v2.0.processed.polygons.csv.gz -t /data/external/natural-earth/polygons/ne_10m_admin_1.regions.v3.0.0.processed.polygons.csv.gz".format(netacq_date)
ipm = pyipmeta.IpMeta(provider="netacq-edge",
                      provider_config=provider_config_str)


IS_COMPRESSED = 1
IS_TEST = 1
num_adjacent_rounds = 1
this_t_round_end = this_t + zeusping_helpers.ROUND_SECS

# Define nested_dicts for loc_asn_to_status files (required for generating timeseries)
def nested_dict_factory_int(): 
  return defaultdict(int)

def nested_dict_factory(): 
  return defaultdict(nested_dict_factory_int)


if read_bin == 1:
    struct_fmt = struct.Struct("I 5H")
    buf = ctypes.create_string_buffer(struct_fmt.size * 2000)

# processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'

if read_bin == 1:
    processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_testbintest3/'.format(campaign)
else:
    processed_op_dir = '/scratch/zeusping/data/processed_op_{0}_testsimple/'.format(campaign)

bitset_cache = {}
for i in range(256):
    bitset_cache[i] = 1<<i

# sys.exit(1) # 3.183G here already

s24_mask = 0
for i in range(24):
    s24_mask |= 1 << i
s24_mask = s24_mask << 8

oct4_mask = (1 << 8) - 1

main()
