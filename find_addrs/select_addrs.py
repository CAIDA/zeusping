
# TODO: Take in the loc and loc pfx.to.gz as input in some file. Then just write a function that accepts a loc and pfx2as file, returns the addresses we care about. I can then have a single output file.

import sys
import random

# loc_to_reqd_asns contains a manual mapping of the ASes that we believe are large residential providers in different areas
# We obtained "likely residential" ASes by finding the ASes with the most addresses in the area and  then checking which of these are "known residential" using Wikipedia and news.

loc_to_reqd_asns = {
    "CO" : {'7922', '209', '7155'},
    "VT" : {'7922', '13977'},
    # "RI" : {'22773', '701', '7029', '7922'},
    "RI" : {'22773', '701'},    
    "CA" : {'7922', '7155'},
    "accra" : {'30986', '37140'},
    }


loc = sys.argv[1]

reqd_asns = loc_to_reqd_asns[loc]

for line in sys.stdin:
    parts = line.strip().split('|')

    if (len(parts) != 2):
        continue
    
    addr = parts[0].strip()
    asn = parts[1].strip()

    # if ( (asn == '7018') or (asn == '22773') or (asn == '20001') ): # SD
    # if ( (asn == '7922') or (asn == '209') or (asn == '11351') or (asn == '10796') or (asn == '20001') or (asn == '7843') or (asn == '11426') or (asn == '33588') or (asn == '12271') ): # Colorado    
    # # if ( (asn == '33588') or (asn == '209') or (asn == '10835') ): # Wyoming
    # # if ( (asn == '7922') or (asn == '701') or (asn == '7018') ): # Philadelphia
    # # if ( (asn == '7018') or (asn == '22773') or (asn == '3372') or (asn == '3375') ): # Tulsa
    #     sys.stdout.write("{0}\n".format(addr) )
    
    if asn in reqd_asns:
        sys.stdout.write("{0}\n".format(addr) )
    

        
    
    
