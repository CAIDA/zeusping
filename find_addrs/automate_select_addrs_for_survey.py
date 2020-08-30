# NOTE: I've hardcoded pfx2as dates for now. Not a great idea for the future...

import sys
import os
import shlex, subprocess
import time
import datetime
import calendar

# reqd_states = ['LA']
reqd_states = []
reqd_states_fp = open(sys.argv[1])
for line in reqd_states_fp:
    reqd_states.append(line[:-1].lower() )

# print reqd_states
# sys.exit(1)

# First find all locids

for st in reqd_states:

    st_u = st.upper()
    st_locids = '{0}_locids'.format(st_u)
    
    locid_cmd = 'zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-locations.latest.csv.gz | python find_locids_given_citystate_netacuity.py us {0} ./data/locids/{1} STATE'.format(st, st_locids)
    sys.stderr.write("Running\n{0}\nat {1}\n".format(locid_cmd, str(datetime.datetime.now() ) ) )
    os.system(locid_cmd)
    
    addr_cmd = 'zcat /data/external/netacuity-dumps/Edge-processed/netacq-4-blocks.latest.csv.gz | python find_addresses_given_locids_netacuity.py ./data/locids/{0} /scratch/zeusping/probelists/us_addrs/{1}_addrs/ all_{1}_addresses'.format(st_locids, st_u)
    sys.stderr.write("Running\n{0}\nat {1}\n".format(addr_cmd, str(datetime.datetime.now() ) ) )
    os.system(addr_cmd)

    # NOTE: Gunzip if necessary.

    pfx2as_cmd = 'ipmeta-lookup -p "pfx2as -f /data/routing/routeviews-prefix2as/2020/08/routeviews-rv2-20200805-0000.pfx2as.gz" -f /scratch/zeusping/probelists/us_addrs/{0}_addrs/all_{0}_addresses | cut -d \| -f 1,16 | gzip | dd of=/scratch/zeusping/probelists/us_addrs/{0}_addrs/all_{0}_addresses_20200805.pfx2as.gz'.format(st_u)
    sys.stderr.write("Running\n{0}\nat {1}\n".format(pfx2as_cmd, str(datetime.datetime.now() ) ) )
    os.system(pfx2as_cmd)

