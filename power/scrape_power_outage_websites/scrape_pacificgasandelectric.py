
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

    # br = mechanize.Browser()
    # br.set_handle_robots(False)
    # br.open('https://apim.pge.com/cocoutage/outages/getOutagesRegions?regionType=city&expand=true&_={0}'.format(writing_time) )
    # soup = BeautifulSoup(br.response().read(), "html5lib")
    # resp = br.response().read()
    # json_stuff = json.loads(resp)
    # print json_stuff
    # for outageregion in json_stuff["outagesRegions"]:
    #     print

    po_url = 'https://apim.pge.com/cocoutage/outages/getOutagesRegions?regionType=city&expand=true&_={0}'.format(writing_time)
    scrape_helper.simplest_scrape(po_url, writing_time, pacificgasandelectric_op_fp)

    # Old way, before scrape_helper    
    # resp = requests.get('https://apim.pge.com/cocoutage/outages/getOutagesRegions?regionType=city&expand=true&_={0}'.format(writing_time) )
    # data = resp.json()
    # data["writing_time"] = writing_time
    # json.dump(data, pacificgasandelectric_op_fp)
    # # json.dump(resp.content, pacificgasandelectric_op_fp)
    # pacificgasandelectric_op_fp.write("\n")


# est = pytz.timezone('America/New_York')
# utc = pytz.timezone('UTC')
# pacificgasandelectric_op_fp = open('/fs/nm-thunderping/weather_alert_prober_logs_master_copy/power_outages_data/pacificgasandelectric_power_outages', 'a')
writing_time = int(time.time())
dt = datetime.datetime.utcfromtimestamp(writing_time)
fdir = '/scratch/zeusping/power/pacificgasandelectric/year={0}/month={1}/day={2}/hour={3}/'.format(dt.year, dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H") )
# mkdir_cmd = 'mkdir -p {0}'.format(fdir)
try:
    os.makedirs(fdir)
except OSError:
    pass
    
fname = "{0}{1}".format(fdir, writing_time)
pacificgasandelectric_op_fp = open(fname, 'w')
# if __name__ == '__main__':
#     while (True):
main(writing_time)
pacificgasandelectric_op_fp.flush()
pacificgasandelectric_op_fp.close()
