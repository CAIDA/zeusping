
import sys

reqd_ctry = sys.argv[1]
op_fname = sys.argv[2]

ctry_loc_ids = set()

line_ct = 0

for line in sys.stdin:
    parts = line.strip().split(',')

    line_ct += 1
    
    if (line_ct == 1):
        continue
    
    locid = int(parts[0].strip() )
    ctry_code = parts[1].strip()

    if (ctry_code == reqd_ctry):
        ctry_loc_ids.add(locid)

        
op_fp = open(op_fname, 'w')
for locid in ctry_loc_ids:
    # sys.stdout.write("{0}\n".format(locid) )
    op_fp.write("{0}\n".format(locid) )
