
import sys

us_loc_ids = set()

line_ct = 0

for line in sys.stdin:
    parts = line.strip().split(',')

    line_ct += 1
    
    if (line_ct == 1):
        continue
    
    locid = int(parts[0].strip() )
    ctry_code = parts[1].strip()

    if (ctry_code == 'us'):
        us_loc_ids.add(locid)


for locid in us_loc_ids:
    sys.stdout.write("{0}\n".format(locid) )
