
# This script will read all IP-level files in a directory (with the assumption that all files in that directory will contain one IP address per line). It will read all the addresses, add their /24s to a set and eventually print out a list of /24s

import sys
import glob
import datetime

inp_dir = sys.argv[1]
op_fp = open(sys.argv[2], 'w')

op_s24s = set()

# List all the files in inp_dir
ip_fnames = glob.glob('{0}/*'.format(inp_dir) )

line_ct = 0
for ip_fname in ip_fnames:
    sys.stdout.write("Reading ip_fname: {0}\n".format(ip_fname) )
    ip_fp = open(ip_fname)

    for line in ip_fp:

        line_ct += 1
        
        ip = line[:-1]

        if line_ct%100000 == 0:
            sys.stderr.write("Done with {0} lines of {1} at: {2}\n".format(line_ct, ip_fname, str(datetime.datetime.now() ) ) )
        

        parts = ip.strip().split('.')
        s24 = "{0}.{1}.{2}.0/24".format(parts[0], parts[1], parts[2])

        op_s24s.add(s24)

    ip_fp.close()


for s24 in op_s24s:
    op_fp.write("{0}\n".format(s24) )
