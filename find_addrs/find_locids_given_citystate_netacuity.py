
import sys

reqd_loc_ids = set()

line_ct = 0

reqd_ctry = sys.argv[1]
reqd_loc = sys.argv[2]
op_fname = sys.argv[3]
state_or_city = sys.argv[4]

for line in sys.stdin:
    parts = line.strip().split(',')

    line_ct += 1
    
    if (line_ct == 1):
        continue
    
    locid = int(parts[0].strip() )
    ctry_code = parts[1].strip()

    if ctry_code != reqd_ctry:
        continue
    # if (ctry_code != 'us'):
    #     continue

    if state_or_city == "CITY":
        city_code = parts[3].strip()

        if city_code == reqd_loc:
            reqd_loc_ids.add(locid)

    elif state_or_city == "STATE":
        state_code = parts[2].strip()

        if state_code == reqd_loc:
            reqd_loc_ids.add(locid)

    elif state_or_city == "CITYSTATE":
        state_code = parts[2].strip()
        city_code = parts[3].strip()

        reqd_state = sys.argv[5]
        
        if state_code == reqd_state and city_code == reqd_loc:
            reqd_loc_ids.add(locid)


op_fp = open(op_fname, 'w')
for locid in reqd_loc_ids:
    op_fp.write("{0}\n".format(locid) )
