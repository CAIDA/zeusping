
import sys
import time
import datetime
import dateutil
import calendar

import json
import pprint

import power_outage_parser


ip_fp = open(sys.argv[1], 'r')


def parse_outagesRegions(outregs):
    for reg in outregs:
        pass
        # print reg['customersAffected']


# def update_regional_outages(reg, regionName, customersAffected, written_time):
#     if regionName not in ongoing_regional_outages:
#         # This regional outage has just begun
#         ongoing_regional_outages[regionName] = {"start" : written_time, "last" : written_time, "custs" : customersAffected}
#     else:
#         if written_time - ongoing_regional_outages[regionName]["last"] <= 1800:
#             # It has been at most 30 minutes since the last outage, so this is just part of the same ongoing outage
#             ongoing_regional_outages[regionName]["last"] = written_time
#             if customersAffected > ongoing_regional_outages[regionName]["custs"]:
#                 ongoing_regional_outages[regionName]["custs"] = customersAffected
#         else:
#             # It has been more than 30 minutes since the last outage. Write old outage and start new outage                           
#             regional_outages.append({"start" : ongoing_regional_outages[regionName]["start"], "end" : ongoing_regional_outages[regionName]["last"], "custs" : ongoing_regional_outages[regionName]["custs"], "regionName" : regionName})
#             ongoing_regional_outages[regionName] = {"start" : written_time, "last" : written_time, "custs" : customersAffected}
                        

def update_outages(reg, regionName, written_time):
    if 'numOutages' in reg:
        if reg['numOutages'] > 0:
            if 'outages' in reg:
                for out in reg['outages']:
                    if ( ('outageNumber' in out) ):
                        outageNumber = out['outageNumber']
                        if 'estCustAffected' in out:
                            estCustAffected = int(out['estCustAffected'])
                            if estCustAffected >= power_outage_parser.MIN_THRESH:
                                if outageNumber not in int_outages:
                                    int_outages[outageNumber] = {'estCustAffected' : estCustAffected, 'cause' : out['cause'], 'outageStartTime' : out['outageStartTime'], 'regionName' : reg['regionName'], 'written_time' : written_time}
                                else:
                                    if estCustAffected >= int_outages[outageNumber]['estCustAffected']:
                                        int_outages[outageNumber] = {'estCustAffected' : estCustAffected, 'cause' : out['cause'], 'outageStartTime' : out['outageStartTime'], 'regionName' : reg['regionName'], 'written_time' : written_time}
    


last_written_time = -1

ongoing_regional_outages = {}
regional_outages = []

int_outages = {}

# MIN_THRESH = 1000
# GAP_THRESH = 86400

for line in ip_fp:
    data = json.loads(line)

    # Pretty printing json output
    print json.dumps(data, indent=4, sort_keys=True)

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
                    if customersAffected < power_outage_parser.MIN_THRESH:
                        continue

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

op_fp = open(sys.argv[2], 'w')
for out in int_outages:
    op_fp.write("{0}|{1}|{2}|{3}\n".format(int_outages[out]['outageStartTime'], int_outages[out]['regionName'], int_outages[out]['estCustAffected'], int_outages[out]['cause']) )
# print int_outages

regional_op_fp = open(sys.argv[3], 'w')
for out in regional_outages:
    dur = out["end"] - out["start"]
    regional_op_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(out["start"], out["end"], out["regionName"], out["custs"], dur) )
