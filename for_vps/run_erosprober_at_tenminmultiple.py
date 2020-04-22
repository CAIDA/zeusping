
import sys
import os
import time
import datetime
import shlex
import subprocess

addr_file = sys.argv[1]
vp = sys.argv[2]

if addr_file == 'CO_VT_RI':
    vp_to_oppref = {
        'ohio1' : 'awsohio1',
        'ncal1' : 'awsncal1',
        'nvir1' : 'awsnvir1',
    }
elif addr_file == 'iran_addrs':
    vp_to_oppref = {
        'bahr1' : 'awsbahr1',
        'mumb1' : 'awsmumb1',
        'soul1' : 'awssoul1',
        'nvir1' : 'awsnvir1',
    }

addr_file_sorted = '{0}_{1}'.format(addr_file, vp)

# Python2 with os.system which is deprecated
# sort_cmd = "sort -R {0} > {1}".format(addr_file, addr_file_sorted)
# sys.stderr.write("{0}\n".format(sort_cmd) )
# os.system(sort_cmd)

# The following will only work in Python3
sort_cmd = "sort -R {0}".format(addr_file)
args = shlex.split(sort_cmd)
print(args)
with open(addr_file_sorted, "w") as outfile:
    try:
        subprocess.run(args, stdout=outfile)
    except:
        sys.stderr.write("Sort command failed; exiting\n")
        sys.exit(1)
        

curr_time = int(time.time())
# print(curr_time)
multiple_of_ten = int(curr_time/600)
# print(multiple_of_ten)
next_multiple = multiple_of_ten*600 + 600
# print(next_multiple)

time_to_sleep = next_multiple - curr_time - 2 # Because erosprober typically takes a couple of seconds to launch
# print time_to_sleep
sys.stderr.write("Going to sleep for {0} seconds until multiple of 10\n".format(time_to_sleep) )
time.sleep(time_to_sleep)

# scamper_cmd = "sudo /home/ubuntu/scamper_2019/bin/sc_erosprober -U scamper_socket -a CO_VT_RI -o /home/ubuntu/zeusping/for_testing/op_CO_VT_RI/awsohio -I600 -R600 -c 'ping -c 1'"

scamper_cmd = "sudo /home/ubuntu/scamper_2019/bin/sc_erosprober -U scamper_socket -a {0} -o /home/ubuntu/zeusping/for_testing/op_{1}/{2} -I600 -R600 -c 'ping -c 1'".format(addr_file_sorted, addr_file, vp_to_oppref[vp])
sys.stderr.write("{0}\n".format(scamper_cmd) )

args = shlex.split(scamper_cmd)

try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Scamper command failed; exiting\n")
    sys.exit(1)
