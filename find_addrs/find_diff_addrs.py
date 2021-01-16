
# Given a set of addresses in one month (m1; the more recent one) and a set of addresses in another (m2; the older one), find the addresses that were in m2 but not in m1. Lookup the pfx2as in m2 to determine which ASes these addresses belonged to.

import sys
import wandio
from collections import defaultdict

def populate_set(fname):
    s = set()
    fp = open(fname)

    for line in fp:
        s.add(line[:-1])

    return s

m1_addrs = populate_set(sys.argv[1])

print "Number of m1 addrs is: {0}".format(len(m1_addrs) )

m2_addrs = populate_set(sys.argv[2])

print "Number of m2 addrs is: {0}".format(len(m2_addrs) )

intersect = m1_addrs & m2_addrs

print "Intersect size is: {0}".format(len(intersect))

diff = m2_addrs - m1_addrs

print "Diff size is {0}".format(len(diff) )

m2_pfx2as_fp = wandio.open(sys.argv[3])

asn_to_diff_ct = defaultdict(int)
for line in m2_pfx2as_fp:
    parts = line.strip().split('|')
    addr = parts[0].strip()

    if addr in diff:
        asn = parts[1].strip()
        asn_to_diff_ct[asn] += 1

sorted_tuple = sorted(asn_to_diff_ct.items(), key=lambda kv: kv[1])
for entry in sorted_tuple:
    sys.stderr.write("{0} {1}\n".format(entry[0], entry[1]))
    
