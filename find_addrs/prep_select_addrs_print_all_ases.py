
import sys
import wandio
from collections import defaultdict

asn_to_ct = defaultdict(int)
fp = wandio.open(sys.argv[1], 'r')
for line in fp:
    parts = line.strip().split('|')
    asn = parts[1].strip()

    if len(asn) == 0:
        continue

    asn_to_ct[asn] += 1

sys.stderr.write("Number of ASes: {0}\n\n".format(len(asn_to_ct) ) )

sorted_tuple = sorted(asn_to_ct.items(), key=lambda kv : kv[1], reverse=True)

for entry in sorted_tuple:
    sys.stdout.write("{0}:1-".format(entry[0]) )
