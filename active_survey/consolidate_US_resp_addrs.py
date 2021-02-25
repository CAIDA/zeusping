
import sys
from collections import defaultdict

inp_fp = open(sys.argv[1])

asn_to_status = {}
for line in inp_fp:
    parts = line.strip().split('|')
    asn = parts[1].strip()

    if asn not in asn_to_status:
        asn_to_status[asn] = {"resp" : 0, "unresp" : 0}
    
    resp = int(parts[2].strip() )
    unresp = int(parts[3].strip() )
    asn_to_status[asn]["resp"] += resp
    asn_to_status[asn]["unresp"] += unresp


op_fp = open(sys.argv[2], 'w')
for asn in asn_to_status:
    tot_resp = asn_to_status[asn]["resp"]
    tot_unresp = asn_to_status[asn]["unresp"]

    resp_pct = (tot_resp/float(tot_resp + tot_unresp)) * 100.0

    op_fp.write("US|{0}|{1}|{2}|{3}|{4:.4f}\n".format(asn, tot_resp, tot_unresp, (tot_resp + tot_unresp), resp_pct) )
