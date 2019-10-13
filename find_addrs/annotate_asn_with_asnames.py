
import sys
# import wandio
import requests
import datetime

# ip_fp = open(sys.argv[1], 'r')
# op_aggr_by_asn_fp = wandio.open(sys.argv[1], 'w')
# op_aggr_by_asname_fp = wandio.open(sys.argv[2], 'w')

op_aggr_by_asn_fp = open(sys.argv[1], 'w')
op_aggr_by_asname_fp = open(sys.argv[2], 'w')
op_asname_to_asns_fp = open(sys.argv[3], 'w')

asname_to_addrs = {}
asname_to_asns = {}

line_ct = 0

for line in sys.stdin:

    line_ct += 1
    if (line_ct % 100 == 0):
        sys.stderr.write("Done with {0} lines at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )

        # if (line_ct % 2500 == 0):
        #     break

    parts = line.strip().split()

    num_addrs = int(parts[0].strip() )

    if (len(parts) == 2):
        asn = parts[1].strip()

        req = 'https://ioda.caida.org/metadata/lookup/asn/{0}'.format(asn)
        resp = requests.get(req)

        if resp.status_code == 200:
            # print resp.json()
            resp_json = resp.json()
            # asname = resp_json["data"]["metadata"][0]["attrs"]["name"] # This seems to be the raw ASname
            # asname = resp_json["data"]["metadata"][0]["name"] # This seems to be a concatenation of org and the AS number
            
#             print line
#             print resp_json["data"]["metadata"]

            if ( (len(resp_json["data"]["metadata"]) > 0) ):

                if "attrs" in resp_json["data"]["metadata"][0] and "org" in resp_json["data"]["metadata"][0]["attrs"]:

                    asname = resp_json["data"]["metadata"][0]["attrs"]["org"] # This seems to be Brad's org dataset
                    asname = asname.encode('utf-8')
                    op_aggr_by_asn_fp.write("{0} {1} {2}\n".format(asn, asname, num_addrs) )

                    if asname not in asname_to_addrs:
                        asname_to_addrs[asname] = 0
                    asname_to_addrs[asname] += num_addrs

                    if asname not in asname_to_asns:
                        asname_to_asns[asname] = set()
                    asname_to_asns[asname].add(asn)

                else:
                    sys.stderr.write("No org data for ASN: {0}\n".format(asn) )
            else:
                sys.stderr.write("No metadata for ASN: {0}\n".format(asn) )
        else:

            sys.stderr.write("Status code not 200 for ASN: {0}\n".format(asn) )
            # sys.exit(1)


for asname in asname_to_addrs:
    op_aggr_by_asname_fp.write("{0} {1}\n".format(asname, asname_to_addrs[asname]) )


for asname in asname_to_asns:
    op_asname_to_asns_fp.write("{0} ".format(asname) )
    for asn in asname_to_asns[asname]:
        op_asname_to_asns_fp.write("{0}-".format(asn) )
    op_asname_to_asns_fp.seek(-1, 1)
    op_asname_to_asns_fp.write("\n")
