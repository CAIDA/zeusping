
#  This software is Copyright (c) 2020 The Regents of the University of
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


