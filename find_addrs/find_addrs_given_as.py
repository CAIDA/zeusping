
import sys
import ipaddress

reqd_asn = sys.argv[1]

for line in sys.stdin:
    parts = line.strip().split()

    pfx = parts[0].strip()
    pfx_len = int(parts[1].strip() )
    asn = parts[2].strip()

    if asn == reqd_asn:
        unicode_str = unicode('{0}/{1}'.format(pfx, pfx_len) )
        net = ipaddress.ip_network(unicode_str)

        for addr in net:
            sys.stdout.write("{0}\n".format(addr) )
        
