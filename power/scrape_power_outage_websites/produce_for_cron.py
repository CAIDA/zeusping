
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

to_be_crond = sys.argv[1][:-3]
cron_machine = sys.argv[2]
# op_fname = sys.argv[3]

for_cron_fp = open('for_cron_{0}.sh'.format(to_be_crond), 'w')

for_cron_fp.write('#!/bin/bash\n\n')

# for_cron_fp.write('mkdir -p "$HOME/ispsurvivor/src/power/scrape_power_outage_websites/tmp/"\n')
for_cron_fp.write('mkdir -p "$HOME/zeusping/power/scrape_power_outage_websites/tmp/"\n')
for_cron_fp.write('PIDFILE="$HOME/zeusping/power/scrape_power_outage_websites/tmp/{0}.pid"\n\n'.format(to_be_crond) )

for_cron_fp.write('if [ -e "${PIDFILE}" ] && (ps -u $(whoami) -opid= |\n')
for_cron_fp.write('        grep -P "^\s*$(cat ${PIDFILE})$" &> /dev/null); then\n')
for_cron_fp.write('    echo "Already running."\n')
for_cron_fp.write('    exit 99\n')
for_cron_fp.write('fi\n\n')

if (cron_machine == 'sprinkles'):
    for_cron_fp.write('cd /nmhomes/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/\n')
    # for_cron_fp.write('python {0}.py {1} > {0}.log &\n\n'.format(to_be_crond, op_fname) )
    for_cron_fp.write('python {0}.py > {0}.log &\n\n'.format(to_be_crond) )
elif (cron_machine == 'bluepill'):
    for_cron_fp.write('cd /home/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/\n')
    # for_cron_fp.write('/usr/local/bin/python {0}.py {1} > {0}.log &\n\n'.format(to_be_crond, op_fname) )
    for_cron_fp.write('/usr/local/bin/python {0}.py > {0}.log &\n\n'.format(to_be_crond) )
elif (cron_machine == 'rain'):
    for_cron_fp.write('cd /home/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/\n')
    # for_cron_fp.write('python {0}.py {1} > {0}.log &\n\n'.format(to_be_crond, op_fname) )
    for_cron_fp.write('python {0}.py > {0}.log &\n\n'.format(to_be_crond) )
elif (cron_machine == 'zeus'):
    for_cron_fp.write('cd /home/ramapad/zeusping/power/scrape_power_outage_websites/\n')
    for_cron_fp.write('python {0}.py > {0}.log &\n\n'.format(to_be_crond) )

for_cron_fp.write('echo $! > "${PIDFILE}"\n')
for_cron_fp.write('chmod 644 "${PIDFILE}"\n')

for_cron_fp.close()

chmod_cmd = 'chmod a+x for_cron_{0}.sh\n'.format(to_be_crond)
sys.stderr.write(chmod_cmd)
os.system(chmod_cmd)

sys.stderr.write('\nCrontab command follows:\n')
if (cron_machine == 'bluepill'):
    sys.stderr.write('*/5 * * * * /home/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/for_cron_{0}.sh >> /home/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/for_cron_{0}.log 2>&1'.format(to_be_crond) )
elif (cron_machine == 'rain'):
    sys.stderr.write('*/5 * * * * /home/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/for_cron_{0}.sh >> /home/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/for_cron_{0}.log 2>&1'.format(to_be_crond) )
elif (cron_machine == 'sprinkles'):
    sys.stderr.write('*/5 * * * * /nmhomes/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/for_cron_{0}.sh >> /nmhomes/ramapad/ispsurvivor/src/power/scrape_power_outage_websites/for_cron_{0}.log 2>&1'.format(to_be_crond) )
elif (cron_machine == 'zeus'):
    sys.stderr.write('*/5 * * * * /home/ramapad/zeusping/power/scrape_power_outage_websites/for_cron_{0}.sh >> /home/ramapad/zeusping/power/scrape_power_outage_websites/for_cron_{0}.log 2>&1'.format(to_be_crond) )
