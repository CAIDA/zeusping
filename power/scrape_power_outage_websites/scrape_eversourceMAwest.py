
import sys
import time
import datetime
import dateutil
import calendar

import json
import requests
import os

import xml.etree.ElementTree

import scrape_helper

def main(writing_time):

    metadata_url = 'https://outagemap.eversource.com/resources/data/external/interval_generation_data/metadata.json?_={0}588'.format(writing_time)
    direc = scrape_helper.scrape_metadata_json(metadata_url, writing_time)

    region_level_url = 'https://outagemap.eversource.com/resources/data/external/interval_generation_data/{0}/report_west.json?_={1}627'.format(direc, writing_time)
    scrape_helper.simplest_scrape(region_level_url, writing_time, eversourceMAwest_region_op_fp)


writing_time = int(time.time())
dt = datetime.datetime.utcfromtimestamp(writing_time)
fdir = '/scratch/zeusping/power/eversourceMAwest/year={0}/month={1}/day={2}/hour={3}/'.format(dt.year, dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H") )
try:
    os.makedirs(fdir)
except OSError:
    pass

region_fname = "{0}{1}_region_eversourceMAwest".format(fdir, writing_time)
eversourceMAwest_region_op_fp = open(region_fname, 'a')
main(writing_time)
eversourceMAwest_region_op_fp.flush()
eversourceMAwest_region_op_fp.close()
