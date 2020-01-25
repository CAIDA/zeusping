
# Use this script if we want to sample addresses from an aggregate (i.e., we don't want to choose all addresses, but say, want to sample about 4 of them)

import sys
import random

op_fname_pref = sys.argv[1]
num_splits = int(sys.argv[2])

op_fp = {}

for sp in range(num_splits):
    op_fname = "{0}_numsp{1}_sp{2}".format(op_fname_pref, num_splits, sp) 
    op_fp[sp] = open(op_fname, 'w')

for line in sys.stdin:
    parts = line.strip().split('|')

    if (len(parts) != 2):
        continue
    
    addr = parts[0].strip()
    asn = parts[1].strip()

    if ( (asn == '7018') or (asn == '22773') or (asn == '20001') ):    
    # if ( (asn == '20001') ):
        # num = random.randint(1, 500)
        # num = random.randint(1, 3)
        # num = random.randint(1, 10)

        num = random.randint(1, num_splits)

        op_fp[num-1].write("{0}\n".format(addr) )
        # if ( (num == 5) or (num == 6) ):
        # if ( (num == 2) ):            
        #     sys.stdout.write("{0}\n".format(addr) )

        
    
    
