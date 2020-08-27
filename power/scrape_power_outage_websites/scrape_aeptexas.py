
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

    metadata_url = 'http://outagemap.aeptexas.com.s3.amazonaws.com/resources/data/external/interval_generation_data/metadata.json?_={0}'.format(writing_time)
    direc = scrape_helper.scrape_metadata_json(metadata_url, writing_time)

    county_level_url = 'http://outagemap.aeptexas.com.s3.amazonaws.com/resources/data/external/interval_generation_data/{0}/report_county.json?_={1}'.format(direc, writing_time)
    scrape_helper.simplest_scrape(county_level_url, writing_time, aeptexas_county_op_fp)

    zip_level_url = 'http://outagemap.aeptexas.com.s3.amazonaws.com/resources/data/external/interval_generation_data/{0}/report_zip.json?_={1}'.format(direc, writing_time)
    scrape_helper.simplest_scrape(zip_level_url, writing_time, aeptexas_zip_op_fp)


writing_time = int(time.time())
dt = datetime.datetime.utcfromtimestamp(writing_time)
fdir = '/scratch/zeusping/power/aeptexas/year={0}/month={1}/day={2}/hour={3}/'.format(dt.year, dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H") )
try:
    os.makedirs(fdir)
except OSError:
    pass

county_fname = "{0}{1}_county".format(fdir, writing_time)
zip_fname = "{0}{1}_zip".format(fdir, writing_time)
aeptexas_county_op_fp = open(county_fname, 'a')
aeptexas_zip_op_fp = open(zip_fname, 'a')
main(writing_time)
aeptexas_county_op_fp.flush()
aeptexas_county_op_fp.close()
aeptexas_zip_op_fp.flush()
aeptexas_zip_op_fp.close()
