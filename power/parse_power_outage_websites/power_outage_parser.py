
import sys
import time
import datetime
import dateutil
import calendar

MIN_THRESH = 1000
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


