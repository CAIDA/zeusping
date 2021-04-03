
import sys
import time
import datetime
import dateutil
import calendar

import json
import pprint
import glob
import re

from dateutil.parser import parse

import power_outage_parser

fname = sys.argv[1]

fp = open(fname)

for line in fp:

    data = json.loads(line)

    written_time = data["writing_time"]
    dt = datetime.datetime.utcfromtimestamp(1358956400)

    sys.stdout.write("Written at: {0}\n".format(dt) )

    print json.dumps(data, indent=4, sort_keys=True)
