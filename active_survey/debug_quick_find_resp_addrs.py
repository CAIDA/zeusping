
# The goal is to see why there is a mismatch between the output of quick_find_resp_addrs.py and quick_find_resp_addrs_bf24e4999c7267c71adefd904d34e77ac91f0c76.py

# Let's read all the addresses belonging to AS58224 that are responsive, according to new program

import sys

reqd_asn = sys.argv[1]
f1 = sys.argv[2]
f2 = sys.argv[3]

def find_resp_unresp(fname):
    fp = open(fname)
    resp = set()
    unresp = set()
    for line in fp:
        parts = line.strip().split()
        if len(parts) != 4:
            continue

        asn = parts[1]

        if asn != reqd_asn:
            continue
        
        ip = parts[2]

        if parts[3] == 'R':
            resp.add(ip)
        elif parts[3] == 'U':
            unresp.add(ip)

    return resp, unresp


resp_f1, unresp_f1 = find_resp_unresp(f1)
resp_f2, unresp_f2 = find_resp_unresp(f2)

diff_unresp = unresp_f2 - unresp_f1
sys.stderr.write("Diff between unresp f2 and unresp f1: {0}".format(len(diff_unresp) ) )
diff_unresp = unresp_f1 - unresp_f2
sys.stderr.write("Diff between unresp f1 and unresp f2: {0}".format(len(diff_unresp) ) )

diff_resp = resp_f1 - resp_f2
sys.stderr.write("Diff between resp f1 and resp f2: {0}".format(len(diff_resp)) )
diff_resp = resp_f2 - resp_f1
sys.stderr.write("Diff between resp f2 and resp f1: {0}".format(len(diff_resp)) )

for addr in diff_resp:
    sys.stdout.write("{0}\n".format(addr) )
