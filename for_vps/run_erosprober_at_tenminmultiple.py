
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
    scamper_cmd = "sudo /home/zp/scamper/bin/sc_erosprober -U scamper_socket -a {0} -o /home/zp/zeusping/op_{1}/{2} -I600 -R600 -c 'ping -c 1'".format(campaign, campaign, vp_to_oppref[vp])
elif ( (plat == 'lin') or (plat == 'doc') ):
    scamper_cmd = "sudo /home/zp/scamper/bin/sc_erosprober -U scamper_{0}.sock -a {1}sp{0} -o /home/zp/zeusping/op_{1}/{2}{0}{3} -I 600 -R 600 -c 'ping -c 1'".format(split_num, campaign, plat, vp)
sys.stderr.write("{0}\n".format(scamper_cmd) )

args = shlex.split(scamper_cmd)

try:
    subprocess.check_call(args)
except subprocess.CalledProcessError:
    sys.stderr.write("Scamper command failed; exiting\n")
    sys.exit(1)
