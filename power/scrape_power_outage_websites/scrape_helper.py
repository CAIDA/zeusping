
import sys
import time
import datetime
import dateutil
import calendar

import json
import requests

import xml.etree.ElementTree

TIMEOUT_THRESH = 5


def scrape_metadata_json(url, writing_time):
    metadata_resp = requests.get(url, timeout=TIMEOUT_THRESH )
    metadata_data = metadata_resp.json()
    direc = metadata_data['directory']

    return direc


def scrape_metadata_xml(url, writing_time):
    metadata_resp = requests.get(url, timeout=TIMEOUT_THRESH )
    metadata_data_root = xml.etree.ElementTree.fromstring(metadata_resp.text)
    for child in metadata_data_root:
        direc = child.text

    return direc


def simplest_scrape(url, writing_time, op_fp):
    resp = requests.get(url, timeout=TIMEOUT_THRESH)

    data = resp.json()
    data["writing_time"] = writing_time
    json.dump(data, op_fp)
    
    op_fp.write("\n")


def simplest_scrape_csv(url, writing_time, op_fp):

    # I considered converting the csv into json. But decided to just write the parser appropriately

    resp = requests.get(url, timeout=TIMEOUT_THRESH)

    data = resp.text
    op_fp.write("{0},{1}".format(data, writing_time) )
    # data = resp.json()
    # data["writing_time"] = writing_time
    # json.dump(data, op_fp)
    
    op_fp.write("\n")


def simplest_scrape_dont_verify(url, writing_time, op_fp):
    resp = requests.get(url, timeout=TIMEOUT_THRESH, verify=False )

    data = resp.json()
    data["writing_time"] = writing_time
    json.dump(data, op_fp)
    
    op_fp.write("\n")


def simple_scrape(url_pref, op_fp):

    writing_time = int(time.time())

    resp = requests.get('{0}{1}'.format(url_pref, writing_time), timeout=TIMEOUT_THRESH )

    data = resp.json()
    data["writing_time"] = writing_time
    json.dump(data, op_fp)

    op_fp.write("\n")


def scrape_entergy(url, writing_time, op_fp):
    # print url
    resp = requests.get(url, timeout=TIMEOUT_THRESH)

    # print resp.json()
    data = resp.json()
    # data["writing_time"] = writing_time
    json.dump(data, op_fp)
    
    op_fp.write("\n")


