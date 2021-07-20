
#  This software is Copyright (c) 2021 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes,
#  without fee, and without a written agreement is hereby granted, provided that
#  the above copyright notice, this paragraph and the following three paragraphs
#  appear in all copies. Permission to make use of this software for other than
#  academic research and education purposes may be obtained by contacting:
#
#  Office of Innovation and Commercialization
#  9500 Gilman Drive, Mail Code 0910
#  University of California
#  La Jolla, CA 92093-0910
#  (858) 534-5815
#  invent@ucsd.edu
#
#  This software program and documentation are copyrighted by The Regents of the
#  University of California. The software program and documentation are supplied
#  "as is", without any accompanying services from The Regents. The Regents does
#  not warrant that the operation of the program will be uninterrupted or
#  error-free. The end-user understands that the program was developed for research
#  purposes and is advised not to rely exclusively on the program for any reason.
#
#  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
#  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
#  PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
#  THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
#  IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
#  MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

import sys
import time
import datetime
import dateutil
import calendar

import json
import pprint

import power_outage_parser

if sys.argv[3] == "STATE":
    REQD_AGGR = "STATE"
elif sys.argv[3] == "COUNTY":
    REQD_AGGR = "COUNTYNAME"
elif sys.argv[3] == "ZIP":
    REQD_AGGR = "ZIPCODE"
elif sys.argv[3] == "CITY":
    REQD_AGGR = "CITYNAME"
else:
    sys.stderr.write("Enter a valid aggregate for which you require power outages. Valid aggregates are STATE, COUNTY, ZIP, and CITY\n")
    sys.exit(1)


ip_fp = open(sys.argv[1], 'r')


last_written_time = -1

ongoing_loc_outages = {}
loc_outages = []

line_ct = 0

for line in ip_fp:

    line_ct += 1

    if len(line.strip()) == 0:
        continue

    data = json.loads(line)

    written_time = data["writing_time"]

    # Pretty printing json output
    # print json.dumps(data, indent=4, sort_keys=True)
    # sys.exit(1)

    # To find when data was missing
    power_outage_parser.print_missing_data_dates(written_time, last_written_time)
    
    # To check what else was in data (features, fieldAliases, fields, displayFieldName, writing_time)
    # for e in data:
    #     pprint.pprint(e)
    # sys.exit(1)

    # Each json entry consists of:
    # 1. writing_time that I inserted.
    # 2. displayFieldName, which seems to be "STATE" when the line is state,  "COUNTYNAME" when the line is county, "ZIPCODE" when the line is zipcode, and "CITYNAME" when the line is city
    # 3. fields, which is an array. Each element in the array seems to be a column in a database table. For example, "CUSTOMERSOUT" is the "name" with "type" = "esriFieldTypeDouble" and "alias" = CUSTOMERSOUT. I don't think we need this table particularly.
    # 4. fieldAliases seems entirely pointless. It's just a dict mapping the same keys to the same values. Perhaps they wanted to provide an alias for these fields at some point...
    # 5. features is the useful data we want

    if "features" in data:
        if data["displayFieldName"] == REQD_AGGR:
            # loc here could be state, county, zipcode, or city.
            for loc in data["features"]:
                if "attributes" in loc:
                    
                    if REQD_AGGR in loc["attributes"]:
                        loc_name = loc["attributes"][REQD_AGGR]
                    else:
                        loc_name = "NAA"

                    if REQD_AGGR != "STATE":
                        if "STATE" in loc["attributes"]:
                            state_name = loc["attributes"]["STATE"]
                            loc_w_state_name = "{0}-{1}".format(state_name, loc_name)
                    else:
                        loc_w_state_name = loc_name

                    if "CUSTOMERSOUT" in loc["attributes"]:
                        loc_out_custs = int(loc["attributes"]["CUSTOMERSOUT"])
                    else:
                        loc_out_custs = 0

                    if loc_out_custs >= power_outage_parser.MIN_THRESH:
                        power_outage_parser.update_regional_outages(ongoing_loc_outages, loc_outages, loc_w_state_name, loc_out_custs, written_time)


    # Update variables for next round
    last_written_time = written_time


# Find the last written_time
sys.stderr.write("Last written time: {0}\n".format(last_written_time) )

# Flush remaining loc outages
for regionName in ongoing_loc_outages:
    loc_outages.append({"start" : ongoing_loc_outages[regionName]["start"], "end" : ongoing_loc_outages[regionName]["last"], "custs" : ongoing_loc_outages[regionName]["custs"], "regionName" : regionName})

loc_op_fp = open(sys.argv[2], 'w')
for out in loc_outages:
    dur = out["end"] - out["start"]
    loc_op_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(out["start"], out["end"], out["regionName"], out["custs"], dur) )

