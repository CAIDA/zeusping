
import sys
import time
import datetime
import dateutil
import calendar
import re
import glob

STATE_MIN_THRESH = 10000
COUNTY_MIN_THRESH = 1000
GAP_THRESH = 86400


def print_missing_data_dates(written_time, last_written_time):
    if (written_time - last_written_time > GAP_THRESH):

        if last_written_time == -1:
            written_time_dt = datetime.datetime.utcfromtimestamp(written_time)
            sys.stderr.write("{0} {1} 0\n".format(str(written_time_dt), str(written_time_dt) ) )
        else:
            last_written_time_dt = datetime.datetime.utcfromtimestamp(last_written_time)
            written_time_dt = datetime.datetime.utcfromtimestamp(written_time)
            sys.stderr.write("{0} {1} {2:.2f}\n".format(str(last_written_time_dt), str(written_time_dt), (written_time - last_written_time)/float(GAP_THRESH) ) )
        # print written_time - last_written_time, written_time, last_written_time


def update_regional_outages(ongoing_regional_outages, regional_outages, regionName, customersAffected, written_time):
    if regionName not in ongoing_regional_outages:
        # This regional outage has just begun
        ongoing_regional_outages[regionName] = {"start" : written_time, "last" : written_time, "custs" : customersAffected}
    else:
        if written_time - ongoing_regional_outages[regionName]["last"] <= 1800:
            # It has been at most 30 minutes since the last outage, so this is just part of the same ongoing outage
            ongoing_regional_outages[regionName]["last"] = written_time
            if customersAffected > ongoing_regional_outages[regionName]["custs"]:
                ongoing_regional_outages[regionName]["custs"] = customersAffected
        else:
            # It has been more than 30 minutes since the last outage. Write old outage and start new outage                           
            regional_outages.append({"start" : ongoing_regional_outages[regionName]["start"], "end" : ongoing_regional_outages[regionName]["last"], "custs" : ongoing_regional_outages[regionName]["custs"], "regionName" : regionName})
            ongoing_regional_outages[regionName] = {"start" : written_time, "last" : written_time, "custs" : customersAffected}


def get_tstamp_to_fname(po_path, po_company, start_time, end_time, regex_str, tstamp_to_fname):

    aggr_regex = re.compile(regex_str)

    for hour_epoch in range(start_time, end_time, 3600):
        hour_dt = datetime.datetime.utcfromtimestamp(hour_epoch)

        # fdir = '/scratch/zeusping/power_aug2020_to_mar2021/{0}/year={1}/month={2}/day={3}/hour={4}/*'.format(po_company, hour_dt.year, hour_dt.strftime("%m"), hour_dt.strftime("%d"), hour_dt.strftime("%H") )    
        fdir = '{0}/{1}/year={2}/month={3}/day={4}/hour={5}/*'.format(po_path, po_company, hour_dt.year, hour_dt.strftime("%m"), hour_dt.strftime("%d"), hour_dt.strftime("%H") )

        unsorted_files = glob.glob(fdir)

        for fil in unsorted_files:
            matched_stuff = aggr_regex.search(fil)

            if matched_stuff is None:
                # sys.stdout.write("Did not match: {0}\n".format(fil) )
                continue

            # sys.stdout.write("Matched: {0}\n".format(fil) )

            this_tstamp = matched_stuff.group(1)
            # sys.stdout.write("{0}\n".format(this_fname) )

            tstamp_to_fname[this_tstamp] = fil

        # sys.exit(1)

    sorted_d = sorted(tstamp_to_fname.items(), key=lambda kv: kv[0])

    return sorted_d


