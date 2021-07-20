
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

# NOTE: I can use this script to identify both county and zip-code-level outages

import sys
import time
import datetime
import dateutil
import calendar

import json
import pprint
import glob
import re

import power_outage_parser

# Instead of reading a single file, I will now take in a begin_time and an end_time and the name of the power company. I will also take in whether this is county/zip
# Let us assume for now that we've already downloaded all the values associated with this particular power company from Swift
# We'll need to open a new file for every hour. So do some datetime magic. Perhaps use epochtime, keep incrementing by 3600 and convert into dt
# Do an ls inside the folder. Find all files. Sort them. Then open them in sorted order.

po_path = sys.argv[1]
po_company = sys.argv[2]
start_time = int(sys.argv[3]) # Start hour that we are interested in
end_time = int(sys.argv[4]) # End hour that we are interested in
aggr = sys.argv[5] # County/zip etc.
state_op_fname = sys.argv[6]
county_op_fname = sys.argv[7]

regex_str = '(\d{10}).*' + aggr + "|" + aggr + '.*(\d{10})'
# print regex_str

tstamp_to_fname = {}

sorted_d = power_outage_parser.get_tstamp_to_fname(po_path, po_company, start_time, end_time, regex_str, tstamp_to_fname)

last_written_time = -1

ongoing_state_outages = {}
state_outages = []

ongoing_county_outages = {}
county_outages = []

file_ct = 0

for elem in sorted_d:
    sys.stderr.write("Processing {0}\n".format(elem[1]) )

    fp = open(elem[1])
    file_ct += 1
    
    for line in fp:

        data = json.loads(line)

        written_time = data["writing_time"]

        # Pretty printing json output
        # print json.dumps(data, indent=4, sort_keys=True)
        # sys.exit(1)

        # To find when data was missing
        power_outage_parser.print_missing_data_dates(written_time, last_written_time)

        # To check what else was in data (file_data, writing_time, file_title)
        # for e in data:
        #     pprint.pprint(e)
        # sys.exit(1)

        # Each json entry consists of:
        # 1. writing_time that I inserted.
        # 2. file_data which appears to contain all the useful data
        # 3. file_title, which seems useless

        # In file_data, there is only 'areas'
        # for d in data["file_data"]:
        #     pprint.pprint(d)
        # sys.exit(1)

        if "areas" in data["file_data"]:    

            for outer_area in data["file_data"]["areas"]:
            #     print outer_area["area_name"]
            #     # The outer_area["area_name"] appears to be only ever be "AEP"

            # if line_ct == 1000:
            #     sys.exit(1)

                for state in outer_area["areas"]:
                    # print state["area_name"]
                    # Each state["area_name"] is a state. For AEP Texas, it looked like it was only TX.

                    # if line_ct == 1000:
                    #     sys.exit(1)

                    if "cust_a" in state:
                        state_out_custs = int(state["cust_a"]["val"])
                    else:
                        state_out_custs = 0

                    if "area_name" in state:
                        state_name = state["area_name"]
                    else:
                        state_name = "NAA"

                    if state_out_custs >= power_outage_parser.STATE_MIN_THRESH:
                        power_outage_parser.update_regional_outages(ongoing_state_outages, state_outages, state_name, state_out_custs, written_time)


                    for county in state["areas"]:
                        # print county["area_name"]

                        # if line_ct == 1000:
                        #     sys.exit(1)

                        if "cust_a" in county:
                            county_out_custs = int(county["cust_a"]["val"])
                        else:
                            county_out_custs = 0

                        if "area_name" in county:
                            county_name = "{0}-{1}".format(state_name, county["area_name"])
                        else:
                            county_name = "NAA"

                        if county_out_custs >= power_outage_parser.COUNTY_MIN_THRESH:
                            power_outage_parser.update_regional_outages(ongoing_county_outages, county_outages, county_name, county_out_custs, written_time)

        # Update variables for next round
        last_written_time = written_time
        
    # sys.exit(1)

# Find the last written_time
sys.stderr.write("Last written time: {0}\n".format(last_written_time) )

# Flush remaining state outages
for regionName in ongoing_state_outages:
    state_outages.append({"start" : ongoing_state_outages[regionName]["start"], "end" : ongoing_state_outages[regionName]["last"], "custs" : ongoing_state_outages[regionName]["custs"], "regionName" : regionName})

# Flush remaining county outages
for regionName in ongoing_county_outages:
    county_outages.append({"start" : ongoing_county_outages[regionName]["start"], "end" : ongoing_county_outages[regionName]["last"], "custs" : ongoing_county_outages[regionName]["custs"], "regionName" : regionName})

state_op_fp = open(state_op_fname, 'w')
for out in state_outages:
    dur = out["end"] - out["start"]
    state_op_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(out["start"], out["end"], out["regionName"], out["custs"], dur) )

county_op_fp = open(county_op_fname, 'w')
for out in county_outages:
    dur = out["end"] - out["start"]
    county_op_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(out["start"], out["end"], out["regionName"], out["custs"], dur) )
