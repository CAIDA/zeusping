
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
import os

import scrape_helper

def prep_num_str(s):
    p = s.replace(',', '')
    p = p.encode('utf8')
    return p


def main(writing_time):

    county_po_url = 'https://entergy.datacapable.com/datacapable/v1/entergy/EntergyArkansas/county'.format(writing_time)
    scrape_helper.scrape_entergy(county_po_url, writing_time, entergyarkansas_county_op_fp)

    zip_po_url = 'https://entergy.datacapable.com/datacapable/v1/entergy/EntergyArkansas/zip'.format(writing_time)
    scrape_helper.scrape_entergy(zip_po_url, writing_time, entergyarkansas_zip_op_fp)


writing_time = int(time.time())
dt = datetime.datetime.utcfromtimestamp(writing_time)
fdir = '/scratch/zeusping/power/entergyarkansas/year={0}/month={1}/day={2}/hour={3}/'.format(dt.year, dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H") )
# mkdir_cmd = 'mkdir -p {0}'.format(fdir)
try:
    os.makedirs(fdir)
except OSError:
    pass
    
county_fname = "{0}{1}_county_entergyarkansas".format(fdir, writing_time)
zip_fname = "{0}{1}_zip_entergyarkansas".format(fdir, writing_time)
entergyarkansas_county_op_fp = open(county_fname, 'w')
entergyarkansas_zip_op_fp = open(zip_fname, 'w')
# if __name__ == '__main__':
#     while (True):
main(writing_time)
entergyarkansas_county_op_fp.flush()
entergyarkansas_county_op_fp.close()
entergyarkansas_zip_op_fp.flush()
entergyarkansas_zip_op_fp.close()
