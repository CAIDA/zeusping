
# This is a script to select ~10M Iranian addresses out of 12.5M total

import sys
import random
import os

op_fname_pref = sys.argv[1]
num_splits = int(sys.argv[2]) # Number of files into which we should split the addresses

mkdir_cmd = 'mkdir -p ./data/{0}'.format(op_fname_pref)
sys.stderr.write("{0}\n".format(mkdir_cmd) )
os.system(mkdir_cmd)

op_fps = {}
for sp in range(num_splits):
    op_fname = "./data/{0}/{0}_numsp{1}_sp{2}".format(op_fname_pref, num_splits, sp+1) 
    op_fps[sp] = open(op_fname, 'w')

done_addrs = set()

asn_to_addrs = {}

line_ct = 0
for line in sys.stdin:

    line_ct += 1

    if line_ct % 100000 == 0:
        sys.stderr.write("Lines processed: {0}\n".format(line_ct) )
    
    parts = line.strip().split('|')

    if (len(parts) != 2):
        continue
    
    addr = parts[0].strip()
    asn = parts[1].strip()

    if asn not in asn_to_addrs:
        asn_to_addrs[asn] = set()
    asn_to_addrs[asn].add(addr)


for asn in asn_to_addrs:
    for addr in asn_to_addrs[asn]:

        num = random.randint(1, 5)

        if ( (num != 5) and (addr not in done_addrs) ):

            # Address needs to be sampled. Let's decide which file to write it to based on num_splits
            file_num = random.randint(1, num_splits)

            op_fps[file_num-1].write("{0}\n".format(addr) )
            
        done_addrs.add(addr)
        
    
    
