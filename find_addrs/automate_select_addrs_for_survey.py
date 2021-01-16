
import sys
import os
import shlex, subprocess
import time
import datetime
from datetime import timedelta
import calendar


def find_pfx2as_fname():
    today_dt = datetime.datetime.today()
    yday_dt = today_dt - timedelta(days = 1)
    yday_day = yday_dt.strftime("%d") # Note: we need 0-padding
    yday_month = yday_dt.strftime("%m") # Note: we need 0-padding
    yday_year = yday_dt.strftime("%Y")

    # Do an ls inside the directory. Find the closest matching date just before yday.
    pfx2as_fpath = "/data/routing/routeviews-prefix2as/{0}/{1}/".format(yday_year, yday_month)

    all_files = os.listdir(pfx2as_fpath)

    for fil in all_files:
        parts = fil.strip().split('-')

        this_y = parts[2][:4]
        this_m = parts[2][4:6]
        this_d = parts[2][6:]

        if this_y == yday_year and this_m == yday_month and this_d == yday_day:
            return "{0}{1}".format(pfx2as_fpath, fil), "{0}{1}{2}".format(this_y, this_m, this_d)

    # If we didn't find a file, signal it.
    return "NA", "NA"
        

def find_reqd_ctrys():
    reqd_ctrys = []
    reqd_ctrys_fp = open(sys.argv[2])
    for line in reqd_ctrys_fp:
        reqd_ctrys.append(line[:-1].lower() )

    return reqd_ctrys


def find_reqd_states():
    if sys.argv[3] == "allusstates":
        reqd_states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

    else:
        # reqd_states = ['LA']
        reqd_states = []
        reqd_states_fp = open(sys.argv[3])
        for line in reqd_states_fp:
            reqd_states.append(line[:-1].lower() )

    return reqd_states


ctry_or_state = sys.argv[1] # Whether we are running scripts for multiple countries (Iran, Ghana etc.) or for multiple states within the same country (Louisiana, Alabama etc.)

# pfx2as_fname = sys.argv[2] # If we want to obtain the pfx2as_fname as a command line argument we can. I believe it makes more sense to use Python date functions to find today's date and use that to find the pfx2as_date.

pfx2as_fname, pfx2as_datesuf = find_pfx2as_fname()
# print pfx2as_fname, pfx2as_datesuf

if pfx2as_fname == "NA":
    sys.stderr.write("Did not find pfx2as file, exiting\n")
    sys.exit(1)


# Find reqd_states/reqd_ctrys
if ctry_or_state == 'state':

    reqd_ctry = sys.argv[2]
    reqd_ctry = reqd_ctry.lower()

    reqd_states = find_reqd_states()

    # print reqd_states
    # sys.exit(1)

else:
    reqd_ctrys = find_reqd_ctrys()
        

if ctry_or_state == 'state':
    for st in reqd_states:

        st = st.lower()
        
        # First find all locids
        st_u = st.upper()
        st_locids = '{0}_locids'.format(st_u)

        locid_cmd = 'zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-locations.latest.csv.gz | python find_locids_given_citystate_netacuity.py {0} {1} ./data/locids/{2} STATE'.format(reqd_ctry, st, st_locids)
        sys.stderr.write("Running\n{0}\nat {1}\n".format(locid_cmd, str(datetime.datetime.now() ) ) )
        os.system(locid_cmd)

        addr_cmd = 'zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-blocks.latest.csv.gz | python find_addresses_given_locids_netacuity.py ./data/locids/{0} /scratch/zeusping/probelists/us_addrs/{1}_addrs/ all_{1}_addresses'.format(st_locids, st_u)
        sys.stderr.write("Running\n{0}\nat {1}\n".format(addr_cmd, str(datetime.datetime.now() ) ) )
        os.system(addr_cmd)

        # NOTE: Gunzip if necessary.

        pfx2as_cmd = 'ipmeta-lookup -p "pfx2as -f {0}" -f /scratch/zeusping/probelists/{1}_addrs/{2}_addrs/all_{2}_addresses | cut -d \| -f 1,16 | gzip | dd of=/scratch/zeusping/probelists/{1}_addrs/{2}_addrs/all_{2}_addresses_{3}.pfx2as.gz'.format(pfx2as_fname, reqd_ctry, st_u, pfx2as_datesuf)
        sys.stderr.write("Running\n{0}\nat {1}\n".format(pfx2as_cmd, str(datetime.datetime.now() ) ) )
        os.system(pfx2as_cmd)


else:
    for ctry in reqd_ctrys:

        # First find all locids
        ctry_u = ctry.upper()
        ctry_locids = '{0}_locids'.format(ctry_u)

        locid_cmd = 'zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-locations.latest.csv.gz | python find_locids_given_country_netacuity.py {0} ./data/locids/{1}'.format(ctry, ctry_locids)
        sys.stderr.write("Running\n{0}\nat {1}\n".format(locid_cmd, str(datetime.datetime.now() ) ) )
        os.system(locid_cmd)

        addr_cmd = 'zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-blocks.latest.csv.gz | python find_addresses_given_locids_netacuity.py ./data/locids/{0} /scratch/zeusping/probelists/{1}_addrs/ all_{1}_addresses'.format(ctry_locids, ctry)
        sys.stderr.write("Running\n{0}\nat {1}\n".format(addr_cmd, str(datetime.datetime.now() ) ) )
        os.system(addr_cmd)

        # NOTE: Gunzip if necessary.
        
        pfx2as_cmd = 'ipmeta-lookup -p "pfx2as -f {0}" -f /scratch/zeusping/probelists/{1}_addrs/all_{1}_addresses | cut -d \| -f 1,16 | gzip | dd of=/scratch/zeusping/probelists/{1}_addrs/all_{1}_addresses_{2}.pfx2as.gz'.format(pfx2as_fname, ctry, pfx2as_datesuf)
        sys.stderr.write("Running\n{0}\nat {1}\n".format(pfx2as_cmd, str(datetime.datetime.now() ) ) )
        os.system(pfx2as_cmd)
        
