
# This script takes as input a pfx2as file and an ASN.
# It lists all active /24s in that AS

import sys
import wandio

ip_fp = wandio.open(sys.argv[1], 'r')
reqd_asn = sys.argv[2]

op_fname = sys.argv[3]
op_fp = open(op_fname, 'w')

line_ct = 0

s24_set = set()
for line in ip_fp:

    line_ct += 1

    if line_ct % 100000 == 0:
        sys.stderr.write("Lines processed: {0}\n".format(line_ct) )
    
    parts = line.strip().split('|')

    if (len(parts) != 2):
        continue

    addr = parts[0].strip()
    asn = parts[1].strip()

    if asn == reqd_asn:
        parts = addr.strip().split('.')
        s24 = "{0}.{1}.{2}.0/24".format(parts[0], parts[1], parts[2])

        s24_set.add(s24)

        
for s24 in s24_set:
    op_fp.write("{0}\n".format(s24) )

