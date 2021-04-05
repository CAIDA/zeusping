
import sys
import time
import datetime
import dateutil
import calendar

import json
import pprint

import power_outage_parser


def parse_outagesRegions(outregs):
    for reg in outregs:
        pass
        # print reg['customersAffected']


def update_outages(reg, regionName, written_time):
    if 'numOutages' in reg:
        if reg['numOutages'] > 0:
            if 'outages' in reg:
                for out in reg['outages']:
                    if ( ('outageNumber' in out) ):
                        outageNumber = out['outageNumber']
                        if 'estCustAffected' in out:
                            estCustAffected = int(out['estCustAffected'])
                            if estCustAffected >= power_outage_parser.COUNTY_MIN_THRESH:
                                if outageNumber not in int_outages:
                                    int_outages[outageNumber] = {'estCustAffected' : estCustAffected, 'cause' : out['cause'], 'outageStartTime' : out['outageStartTime'], 'regionName' : reg['regionName'], 'written_time' : written_time}
                                else:
                                    if estCustAffected >= int_outages[outageNumber]['estCustAffected']:
                                        int_outages[outageNumber] = {'estCustAffected' : estCustAffected, 'cause' : out['cause'], 'outageStartTime' : out['outageStartTime'], 'regionName' : reg['regionName'], 'written_time' : written_time}
    

po_path = sys.argv[1]
po_company = sys.argv[2]
start_time = int(sys.argv[3]) # Start hour that we are interested in
end_time = int(sys.argv[4]) # End hour that we are interested in
op_fname = sys.argv[5]
regional_op_fname = sys.argv[6]

regex_str = '(\d{10}).*'

tstamp_to_fname = {}

sorted_d = power_outage_parser.get_tstamp_to_fname(po_path, po_company, start_time, end_time, regex_str, tstamp_to_fname)

last_written_time = -1

ongoing_regional_outages = {}
regional_outages = []

int_outages = {}

# MIN_THRESH = 1000
# GAP_THRESH = 86400

file_ct = 0

for elem in sorted_d:
    sys.stderr.write("Processing {0}\n".format(elem[1]) )

    fp = open(elem[1])
    file_ct += 1
    
    for line in fp:

        data = json.loads(line)

        # Pretty printing json output
        # print json.dumps(data, indent=4, sort_keys=True)

        written_time = data["writing_time"]

        # To find when data was missing
        power_outage_parser.print_missing_data_dates(written_time, last_written_time)

        for e in data:

            # To check what else was in data
            # if (e != 'outagesRegions'):
                # pprint.pprint(e)
            # Each entry consists primarily of several outagesRegions. Other than outagesRegions:
            # 1. there is the writing_time that I inserted.
            # 2. There is something called validationErrorMap
            # 3. There is something called validationErrors

            if (e == 'outagesRegions'):
                # parse_outagesRegions(data[e])
                for reg in data['outagesRegions']:
                    # Each reg in data['outagesRegions'] consists of data for the overall region, such as:
                    # regionName
                    # latitude
                    # longitude
                    # customersAffected
                    # NOTE: Some of these do *not* have customersAffected

                    if 'regionName' in reg:
                        regionName = reg['regionName']
                    else:
                        regionName = 'NAA'

                    # Keep track of regional outages that affect lots of customers

                    # When a region has more than MIN_THRESH customers affected, we should track those times.
                    if 'customersAffected' in reg:
                        customersAffected = int(reg['customersAffected'])
                        if customersAffected >= power_outage_parser.COUNTY_MIN_THRESH:
                            # update_regional_outages(reg, regionName, customersAffected, written_time)
                            power_outage_parser.update_regional_outages(ongoing_regional_outages, regional_outages, regionName, customersAffected, written_time)

                    # Keep track of individual outages that affect lots of customers
                    update_outages(reg, regionName, written_time)


        # Update variables for next round
        last_written_time = written_time
        

sys.stderr.write("Last written time: {0}\n".format(last_written_time) )

# Flush remaining regional outages
for regionName in ongoing_regional_outages:
    regional_outages.append({"start" : ongoing_regional_outages[regionName]["start"], "end" : ongoing_regional_outages[regionName]["last"], "custs" : ongoing_regional_outages[regionName]["custs"], "regionName" : regionName})

op_fp = open(op_fname, 'w')
for out in int_outages:
    op_fp.write("{0}|{1}|{2}|{3}\n".format(int_outages[out]['outageStartTime'], int_outages[out]['regionName'], int_outages[out]['estCustAffected'], int_outages[out]['cause']) )
# print int_outages

regional_op_fp = open(regional_op_fname, 'w')
for out in regional_outages:
    dur = out["end"] - out["start"]
    regional_op_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(out["start"], out["end"], out["regionName"], out["custs"], dur) )
