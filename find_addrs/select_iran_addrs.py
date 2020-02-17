

import sys
import random

op_fname = sys.argv[1]
op_fp = open(op_fname, 'w')

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
        num = random.randint(1, 2)

        if num == 2:
            op_fp.write("{0}\n".format(addr) )

        
    
    
