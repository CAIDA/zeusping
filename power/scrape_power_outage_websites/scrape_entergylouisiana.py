
import sys
import time
import datetime
import dateutil
import calendar

import json
import requests
import os

import scrape_helper

def prep_num_str(s):
    p = s.replace(',', '')
    p = p.encode('utf8')
    return p


def main(writing_time):

    county_po_url = 'https://entergy.utilisocial.io/datacapable/v1/map/events/EntergyLouisiana/county?t={0}'.format(writing_time)
    scrape_helper.scrape_entergy(county_po_url, writing_time, entergylouisiana_county_op_fp)

    zip_po_url = 'https://entergy.utilisocial.io/datacapable/v1/map/events/EntergyLouisiana/zip?t={0}'.format(writing_time)
    scrape_helper.scrape_entergy(zip_po_url, writing_time, entergylouisiana_zip_op_fp)


writing_time = int(time.time())
dt = datetime.datetime.utcfromtimestamp(writing_time)
fdir = '/scratch/zeusping/power/entergylouisiana/year={0}/month={1}/day={2}/hour={3}/'.format(dt.year, dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H") )
# mkdir_cmd = 'mkdir -p {0}'.format(fdir)
try:
    os.makedirs(fdir)
except OSError:
    pass
    
county_fname = "{0}{1}_county".format(fdir, writing_time)
zip_fname = "{0}{1}_zip".format(fdir, writing_time)
entergylouisiana_county_op_fp = open(county_fname, 'w')
entergylouisiana_zip_op_fp = open(zip_fname, 'w')
# if __name__ == '__main__':
#     while (True):
main(writing_time)
entergylouisiana_county_op_fp.flush()
entergylouisiana_county_op_fp.close()
entergylouisiana_zip_op_fp.flush()
entergylouisiana_zip_op_fp.close()
