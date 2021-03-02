
# If we want to use select_addrs.py to obtain addresses belonging to a few ASes from all U.S. states, we would use this script.

import sys

op_fp = open(sys.argv[1], 'w')

asn_list = ['7922', '701', '209', '22394', '1239', '21928']

reqd_states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]



for us_st in reqd_states:
    op_line = "{0} ".format(us_st) 
    
    for asn in asn_list: 
        op_line += "{0}:1-".format(asn)
        
    op_line = op_line[:-1]
    op_fp.write("{0}\n".format(op_line))        
    
