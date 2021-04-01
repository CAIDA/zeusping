
# This script will read all IP-level files in a directory (with the assumption that all files in that directory will contain one IP address per line). It will read all the addresses, add their /24s to a set and eventually print out a list of /24s

import sys
import glob
import datetime
import radix

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers


inp_dir = sys.argv[1]
op_fp = open(sys.argv[2], 'w')

MUST_FIND_AS = 1

if MUST_FIND_AS == 1:
    pfx2AS_fn = '/data/routing/routeviews-prefix2as/2021/02/routeviews-rv2-20210227-1200.pfx2as.gz'
    rtree = radix.Radix()
    rnode = zeusping_helpers.load_radix_tree(pfx2AS_fn, rtree)
    s24_to_asn = {}

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

        if MUST_FIND_AS == 1:
            asn = 'UNK'
            s24_addr1 = "{0}.{1}.{2}.1".format(parts[0], parts[1], parts[2])
            rnode = rtree.search_best(s24_addr1)
            if rnode is None:
                asn = 'UNK'
            else:
                asn = rnode.data["origin"]

            s24_to_asn[s24] = asn

    ip_fp.close()


for s24 in op_s24s:
    op_fp.write("{0}\n".format(s24) )

if MUST_FIND_AS == 1:
    s24_to_as_fp = open("{0}_with_asn".format(sys.argv[2]), 'w')

    for s24 in s24_to_asn:
        s24_to_as_fp.write("{0}|{1}\n".format(s24, s24_to_asn[s24]) )
