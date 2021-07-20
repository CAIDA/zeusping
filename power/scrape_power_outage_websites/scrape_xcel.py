
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

    # NOTE: The original query had an extra parameter: and <unix_ts1>=<unix_ts1>. Like so:
    # https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/2/query?f=json&where=1=1 and 1551999299259=1551999299259&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&cacheBuster=1551999299266
    # I experimented with different values for those fields. When I set those values to not be equal to each other, no data was fetched. But if I set those values to be any values that are equal to each other (even values that are more than a day before current timestamp), the most recent records seem to be fetched. Even better, not setting those values at all will still fetch the most recent record. So I went with not setting the field at all.

    city_level_url = 'https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/1/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&cacheBuster={0}643'.format(writing_time)
    scrape_helper.simplest_scrape(city_level_url, writing_time, xcel_city_op_fp)
    xcel_city_op_fp.write("\n")

    county_level_url = 'https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/2/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&cacheBuster={0}123'.format(writing_time)
    scrape_helper.simplest_scrape(county_level_url, writing_time, xcel_county_op_fp)

    state_level_url = 'https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/3/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&cacheBuster={0}297'.format(writing_time)
    scrape_helper.simplest_scrape(state_level_url, writing_time, xcel_state_op_fp)

    zip_level_url = 'https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/4/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&cacheBuster={0}206'.format(writing_time)
    scrape_helper.simplest_scrape(zip_level_url, writing_time, xcel_zip_op_fp)

    # indi_outages_url = 'https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/0/query?f=json&where=1=1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry={{"xmin":-14626888.414520022,"ymin":4149355.447707913,"xmax":-8467898.423415296,"ymax":6477933.077386904,"spatialReference":{{"wkid":102100}}}}&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&cacheBuster={0}782'.format(writing_time)
    indi_outages_url = 'https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/0/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=BEGINTIME DESC&cacheBuster={0}782'.format(writing_time)
    scrape_helper.simplest_scrape(indi_outages_url, writing_time, xcel_indioutages_op_fp)

    # To get individual outages, at different levels of zoom, these are the dimensions for xmin, ymin, xmax and ymax:
    # Zoom 0
    # https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/0/query?f=json&where=1=1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry={"xmin":-14626888.414520022,"ymin":4149355.447707913,"xmax":-8467898.423415296,"ymax":6477933.077386904,"spatialReference":{"wkid":102100}}&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&cacheBuster=1551999914782

    # x: 6158989.991104724
    # y: 2328577.629678991

    # Another version of zoom 0 with the map positioned at a different place. The differences between xmin and xmax, ymin and ymax are equivalent.
    # https://xcelenergy-ags.esriemcs.com/arcgis/rest/services/XcelOutage/MapServer/0/query?f=json&where=1=1 AND 1552018315531=1552018315531&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry={"xmin":-13878417.033551775,"ymin":3968352.564728664,"xmax":-7719427.042447049,"ymax":6296930.194407654,"spatialReference":{"wkid":102100}}&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&cacheBuster=1552018315533

    # As I zoomed in, the number of returned outages decreased. So I believe that obtaining outages at the least zoomed out level should be sufficient


writing_time = int(time.time())
dt = datetime.datetime.utcfromtimestamp(writing_time)
fdir = '/scratch/zeusping/power/xcel/year={0}/month={1}/day={2}/hour={3}/'.format(dt.year, dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H") )
try:
    os.makedirs(fdir)
except OSError:
    pass

city_fname = "{0}{1}_city_xcel".format(fdir, writing_time)
county_fname = "{0}{1}_county_xcel".format(fdir, writing_time)
state_fname = "{0}{1}_state_xcel".format(fdir, writing_time)
zip_fname = "{0}{1}_zip_xcel".format(fdir, writing_time)
indioutages_fname = "{0}{1}_indioutages_xcel".format(fdir, writing_time)

xcel_city_op_fp = open(city_fname, 'w')
xcel_county_op_fp = open(county_fname, 'w')
xcel_state_op_fp = open(state_fname, 'w')
xcel_zip_op_fp = open(zip_fname, 'w')
xcel_indioutages_op_fp = open(indioutages_fname, 'w')

main(writing_time)

xcel_city_op_fp.flush()
xcel_county_op_fp.flush()
xcel_state_op_fp.flush()
xcel_zip_op_fp.flush()
xcel_indioutages_op_fp.flush()

xcel_city_op_fp.close()
xcel_county_op_fp.close()
xcel_state_op_fp.close()
xcel_zip_op_fp.close()
xcel_indioutages_op_fp.close()
