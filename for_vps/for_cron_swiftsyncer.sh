#!/bin/bash

mkdir -p "$HOME/zeusping/for_testing/tmp"
PIDFILE="$HOME/zeusping/for_testing/tmp/syncer.pid"

if [ -e "${PIDFILE}" ] && (ps -u $(whoami) -opid= |
	grep -P "^\s*$(cat ${PIDFILE})$" &> /dev/null); then
    echo "Already running."
    exit 99
fi

source ~/.limbo-cred
cd /home/ubuntu/zeusping/for_testing/
python swiftsync_all_but_latest.py /home/ubuntu/zeusping/for_testing/op_CO_VT_RI/ > ./swiftsync_all_but_latest.log 2>&1 &

echo $! > "${PIDFILE}"
chmod 644 "${PIDFILE}"
