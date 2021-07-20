
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
import os

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
county_op_fname = sys.argv[6]

MODE = 2020 # TODO: Also write parser for 2021!
state_code_to_std = {
    'T' : 'TX',
    'L' : 'LA',
    'A' : 'AR',
    'M' : 'MS',
}

regex_str = '(\d{10}).*' + aggr + "|" + aggr + '.*(\d{10})'
# print regex_str

tstamp_to_fname = {}

sorted_d = power_outage_parser.get_tstamp_to_fname(po_path, po_company, start_time, end_time, regex_str, tstamp_to_fname)

last_written_time = -1

ongoing_county_outages = {}
county_outages = []

file_ct = 0

tstamp_regex = re.compile('(\d{10}).*')

for elem in sorted_d:
    # sys.stderr.write("Processing {0}\n".format(elem[1]) )

    matched_stuff = tstamp_regex.search(elem[0])
    if matched_stuff is None:
        sys.stdout.write("Did not match: {0}\n".format(elem[0]) )
        sys.exit(1)
        continue
    # sys.stdout.write("Matched: {0}\n".format(elem[0]) )
    written_time = int(matched_stuff.group(1))

    # To find when data was missing
    power_outage_parser.print_missing_data_dates(written_time, last_written_time)

    # I empirically determined that files written before 1607563502 used the earlier format
    if written_time < 1607563502:
        MODE = 'PRE_1607563502'
    else:
        MODE = 'POST_1607563502'
    
    if os.stat(elem[1]).st_size == 0:
        # sys.stderr.write("Empty file at: {0}, {1}\n".format(elem[0], str(datetime.datetime.utcfromtimestamp(written_time) ) ) )
        # last_written_time = written_time
        continue

    fp = open(elem[1])
    file_ct += 1
    
    for line in fp:

        data = json.loads(line)

        if len(data) == 0:
            sys.stderr.write("Empty array at: {0}, {1}\n".format(elem[0], str(datetime.datetime.utcfromtimestamp(written_time) ) ) )
            last_written_time = written_time
            continue

        # written_time = data["writing_time"]

        # Pretty printing json output
        # print json.dumps(data, indent=4, sort_keys=True)
        # sys.exit(1)

        if MODE == 'PRE_1607563502':
            # What we found: Each entry is a list of county (or zip) names along with details about the county (or zip)
            # print data[0][5]

            # NOTE: I initially considered getting the written_time from the first element in the list of outages (which has some timestamp populated by Entergy). However, I decided to go with the timestamp encoded in the file-name instead, since this records the time at which we scraped the page.
            # if aggr == 'county':
            #     written_time_ms = str(data[0][5])
            # elif aggr == 'zip':
            #     written_time_ms = str(data[0][4])
            # written_time = int(written_time_ms[:-3])

            # print written_time
            # sys.exit(1)
            
            for county_info in data:

                if aggr == 'county':
                    state_code = county_info[1]
                    county_name = county_info[2]
                    county_out_custs = int(county_info[4])                    
                elif aggr == 'zip':
                    state_code = county_info[0]
                    county_name = county_info[1]
                    county_out_custs = int(county_info[3])
                std_state_code = state_code_to_std[state_code]

                # if file_ct == 1000:
                #     sys.exit(1)

                county_name = "{0}-{1}".format(std_state_code, county_name)

                if county_out_custs >= power_outage_parser.COUNTY_MIN_THRESH:
                    power_outage_parser.update_regional_outages(ongoing_county_outages, county_outages, county_name, county_out_custs, written_time)

        elif MODE == 'POST_1607563502':
            # What we found: Each entry is a list of county (or zip) data. Each element in the list is a dict with details about the county (or zip)
            for county_info in data:

                if aggr == 'county':
                    county_name = county_info["county"]
                    # print county_name
                    # sys.exit(1)
                elif aggr == 'zip':
                    county_name = county_info["zip"]
                state_code = county_info["state"]
                std_state_code = state_code_to_std[state_code]
                county_out_custs = county_info["customersAffected"]

                county_name = "{0}-{1}".format(std_state_code, county_name)

                if county_out_custs >= power_outage_parser.COUNTY_MIN_THRESH:
                    power_outage_parser.update_regional_outages(ongoing_county_outages, county_outages, county_name, county_out_custs, written_time)
                
        # Update variables for next round
        last_written_time = written_time
        
    # sys.exit(1)

# Find the last written_time
sys.stderr.write("Last written time: {0}\n".format(last_written_time) )

# Flush remaining county outages
for regionName in ongoing_county_outages:
    county_outages.append({"start" : ongoing_county_outages[regionName]["start"], "end" : ongoing_county_outages[regionName]["last"], "custs" : ongoing_county_outages[regionName]["custs"], "regionName" : regionName})

county_op_fp = open(county_op_fname, 'w')
for out in county_outages:
    dur = out["end"] - out["start"]
    county_op_fp.write("{0}|{1}|{2}|{3}|{4}\n".format(out["start"], out["end"], out["regionName"], out["custs"], dur) )
