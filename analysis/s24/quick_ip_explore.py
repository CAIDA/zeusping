
import sys
# import pyipmeta
from collections import namedtuple
import wandio
import datetime
import dateutil
from dateutil.parser import parse
import shlex
import subprocess

inp_path = sys.argv[1]
start = int(sys.argv[2])
end = int(sys.argv[3])
reqd_ip = sys.argv[4]
is_compressed = int(sys.argv[5])

ROUND_SECS = 10 * 60 # Number of seconds in a 10-minute round

for round_tstart in range(start, end, 600):
    if is_compressed == 1:
        inp_fname = "{0}/{1}_to_{2}.gz".format(inp_path, round_tstart, round_tstart + ROUND_SECS )
        inp_fp = wandio.open(inp_fname)
    else:
        inp_fname = "{0}/{1}_to_{2}".format(inp_path, round_tstart, round_tstart + ROUND_SECS )
        inp_fp = open(inp_fname)
        
    for line in inp_fp:
        parts = line.strip().split()

        if len(parts) != 2:
            sys.stderr.write("Weird line length: {0}\n".format(line) )
            sys.exit(1)
    
        addr = parts[0].strip()

        if addr != reqd_ip:
            continue
        
        status = parts[1].strip()

        sys.stdout.write("{0} {1} {2} {3}\n".format(addr, round_tstart, str(datetime.datetime.utcfromtimestamp(round_tstart)), status) )

        
