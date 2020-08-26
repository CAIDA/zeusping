
import sys
import datetime
import time

fp = open(sys.argv[1])
line_ct = 0
for line in fp:
    line_ct += 1

    if line_ct % 100000 == 0:
        sys.stderr.write("Processed {0} lines at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )
        time.sleep(10)
