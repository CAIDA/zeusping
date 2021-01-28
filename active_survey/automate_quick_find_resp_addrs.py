
import sys

import os
import shlex, subprocess
import time
import datetime
import dateutil
from dateutil.parser import parse
import calendar

reqd_states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

for st in reqd_states:

    st = st.lower()

    st_u = st.upper()

    # sys.stdout.write("Beginning at {0}\n".format(str(datetime.datetime.now() ) ) )
    cmd = "python quick_find_resp_addrs.py isi-netacq /scratch/zeusping/probelists/us_addrs/{0}_addrs/all_{0}_addresses_20201227.pfx2as.gz /scratch/zeusping/data/quick_census/isi/isi_20200710_resp_addrs.pfx2as.gz {0}".format(st_u)
    sys.stderr.write("Running\n{0}\nat {1}\n".format(cmd, str(datetime.datetime.now() ) ) )    
    os.system(cmd)
