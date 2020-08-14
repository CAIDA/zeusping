
import sys

regionasn_to_status_fname = sys.argv[1]
regionasn_to_status_fp = open(regionasn_to_status_fname)

op_fname = sys.argv[2]
sampling_factor = int(sys.argv[3])

region_to_reqd_asns = {}
for line in regionasn_to_status_fp:
    parts = line.strip().split()

    region = parts[0].strip()
    asn = parts[1].strip()
    
    num_resp = int(parts[2].strip())
    num_unresp = int(parts[3].strip())

    pct_resp = float(parts[4].strip())
    
    if pct_resp > 10 and num_resp > 1000:
        if region not in region_to_reqd_asns:
            region_to_reqd_asns[region] = set()
            
        region_to_reqd_asns[region].add(asn)


op_fp = open(op_fname, "w")
for region in region_to_reqd_asns:

    asn_str = ''
    for asn in region_to_reqd_asns[region]:
        asn_str += '{0}:{1}-'.format(asn, sampling_factor)

    asn_str = asn_str[:-1]
    op_fp.write("{0} {1}\n".format(region, asn_str) )
