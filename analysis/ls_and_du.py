
# I used this script to debug the census files and identify why some output warts files are much larger than others. Are files at the beginning of larger size and files later of smaller size? I used a combination of ls, du and displayed all file times and sizes.

import sys
import os
import subprocess
import shlex
import datetime

p = sys.argv[1]

for fname in os.listdir(p):
    # print(fname)

    parts = fname.strip().split('.')
    ctime = int(parts[1].strip())

    dt = datetime.datetime.utcfromtimestamp(ctime)

    siz = os.path.getsize("{0}/{1}".format(p, fname) )

    sys.stdout.write("{3} | fname: {0} | dt: {1} | siz : {2} MB\n".format(fname, str(dt), float(siz)/(1028*1028), ctime) )

