
import sys
import os
import time
import datetime
import shlex
import subprocess

campaign = sys.argv[1]
vp = sys.argv[2]
plat = sys.argv[3]
split_num = sys.argv[4] # Which scamper instance to connect to

# if campaign == 'CO_VT_RI':

#     if plat == 'aws':
#         vp_to_oppref = {
#             'ohio1' : 'awsohio1',
#             'ncal1' : 'awsncal1',
#             'nvir1' : 'awsnvir1',
#         }
#     elif plat == 'lin':
#         vp_to_oppref = {
#             'fremont1' : 'linf1',
#             'dallas1' : 'lind1',
#             'newark1' : 'linn1',
#         }
#     elif plat == 'doc':
#         vp_to_oppref = {
#             'nyc1' : 'docn1',
#             'sf1' : 'docs1',
#             'toronto1' : 'doct1',
#         }
        
# elif campaign == 'iran_addrs':
#     vp_to_oppref = {
#         'bahr1' : 'awsbahr1',
#         'mumb1' : 'awsmumb1',
#         'soul1' : 'awssoul1',
#         'nvir1' : 'awsnvir1',
#     }

# elif campaign == 'FL':
#     if plat == 'lin':
#         vp_to_oppref = {
#             'fremont2' : 'linf2',
#             'dallas2' : 'lind2',
#             'newark2' : 'linn2',
#         }

# TODO: If we do mkdir from here, the dir belongs to root. swiftsync (which is run as ramapad) does not work. Figure this out later. Perhaps execute chown on the directory and give it to ramapad or something
# mkdir_cmd = 'mkdir -p ./op_{0}/'.format(campaign)
# sys.stderr.write("{0}\n".format(mkdir_cmd) )
# os.system(mkdir_cmd)
    
curr_time = int(time.time())
# print(curr_time)
multiple_of_ten = int(curr_time/600)
# print(multiple_of_ten)
next_multiple = multiple_of_ten*600 + 600
# print(next_multiple)

time_to_sleep = next_multiple - curr_time - 5 # Because erosprober typically takes a few seconds to launch
# print time_to_sleep
sys.stderr.write("Going to sleep for {0} seconds until multiple of 10\n".format(time_to_sleep) )
time.sleep(time_to_sleep)

# scamper_cmd = "sudo /home/ubuntu/scamper_2019/bin/sc_erosprober -U scamper_socket -a CO_VT_RI -o /home/ubuntu/zeusping/for_testing/op_CO_VT_RI/awsohio -I600 -R600 -c 'ping -c 1'"

if plat == 'aws':
    scamper_cmd = "sudo /home/ubuntu/scamper_2019/bin/sc_erosprober -U scamper_socket -a {0} -o /home/ubuntu/zeusping/for_testing/op_{1}/{2} -I600 -R600 -c 'ping -c 1'".format(campaign, campaign, vp_to_oppref[vp])
elif ( (plat == 'lin') or (plat == 'doc') ):
    scamper_cmd = "sudo /home/ramapad/scamper_2019/bin/sc_erosprober -U scamper_{0}.sock -a {1}sp{0} -o /home/ramapad/zeusping/for_testing/op_{1}/{2}{0}{3} -I 600 -R 600 -c 'ping -c 1'".format(split_num, campaign, plat, vp)
sys.stderr.write("{0}\n".format(scamper_cmd) )

args = shlex.split(scamper_cmd)

try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Scamper command failed; exiting\n")
    sys.exit(1)
