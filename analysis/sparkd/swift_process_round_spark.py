
import sys
import glob
import shlex
import subprocess32
import subprocess
import os
import datetime
import json

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('process_zp_round')
sc = SparkContext(conf=conf)

def update_addr_to_resps(fname):
    wandiocat_cmd = '../swift_wrapper.sh swift://zeusping-warts/{0}'.format(fname)
    print wandiocat_cmd
    args = shlex.split(wandiocat_cmd)

    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, bufsize=-1)
    except:
        sys.stderr.write("wandiocat failed for {0}; exiting\n".format(wandiocat_cmd) )
        sys.exit(1)
        
    line_ct = 0

    addr_to_resps = {}

    # ip_fp = open(fname)
    # for line in ip_fp:
    
    for line in proc.stdout:
        line_ct += 1

        try:
            data = json.loads(line)
        except ValueError:
            print line
            continue

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

    return [(addr, addr_to_resps[addr]) for addr in addr_to_resps]
    

# def merge_results(vals):
#     for val in vals:
        
# I need to populate the list of files that Spark will operatre upon

campaign = sys.argv[1] # CO_VT_RI/FL/iran_addrs

round_tstart = int(sys.argv[2])
round_tend = round_tstart + 600
reqd_round_num = int(round_tstart)/600

# processed_op_dir = '/scratch/zeusping/data/spark_processed_op_{0}'.format(campaign)

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

list_of_files = []
for fname in potential_files.strip().split('\n'):
    parts = fname.strip().split('.warts.gz')
    # print parts
    file_ctime = parts[0][-10:]

    round_num = int(file_ctime)/600

    if round_num == reqd_round_num:
        # We found a warts file that belongs to this round and needs to be processed
        list_of_files.append(fname)


# list_of_files = ['temp1', 'temp2', 'temp3', 'temp4']
parallelized_rdd = sc.parallelize(list_of_files)

raw_results = parallelized_rdd.flatMap(update_addr_to_resps)

reduced_results = raw_results.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4]) )

# temp_values = raw_results.collect()

output = reduced_results.collect()

op_fp = open('temp_op', 'w')
for key, val in output:
    op_fp.write("{0} {1} {2} {3} {4} {5}\n".format(key, val[0], val[1], val[2], val[3], val[4]) )

