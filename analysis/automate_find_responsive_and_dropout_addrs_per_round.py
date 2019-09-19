
import sys
import os
import datetime

tstart = int(sys.argv[1])
tend = int(sys.argv[2])

for this_t in range(tstart, tend, 600):

    py_cmd = 'python find_responsive_and_dropout_addrs_per_round.py {0}'.format(this_t)

    sys.stderr.write("\n\n{0}\n".format(str(datetime.datetime.now() ) ) )
    sys.stderr.write("{0}\n".format(py_cmd) )
    os.system(py_cmd)
    sys.stderr.write("{0}\n\n".format(str(datetime.datetime.now() ) ) )
