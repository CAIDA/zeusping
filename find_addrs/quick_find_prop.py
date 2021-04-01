
# ramapad@zeus:~/zeusping/find_addrs/data$ awk -F '|' '{print $2}' s24s_for_edgecast_IRonly_with_asn | sort | uniq -c | sort -n  | tail -n 20 > temp_tbd
# ramapad@zeus:~/zeusping/find_addrs$ python quick_find_prop.py ./data/temp_tbd

import sys

fp = open(sys.argv[1])

as_list = []
tot_s24s = 0

for line in fp:
    parts = line.strip().split()
    num_s24s = int(parts[0])
    asn = parts[1]

    as_list.append(asn)

    tot_s24s += num_s24s

fp.close()

fp = open(sys.argv[1])
for line in fp:
    parts = line.strip().split()
    num_s24s = int(parts[0])
    asn = parts[1]

    sys.stdout.write("{0} {1} {2:.1f}\n".format(asn, num_s24s, float(num_s24s)*100/tot_s24s) )
