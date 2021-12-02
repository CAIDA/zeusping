#!/bin/bash

mkdir -p "$HOME/zeusping/for_vps/tmp"
PIDFILE="$HOME/zeusping/for_vps/tmp/rsyncer.pid"

if [ -e "${PIDFILE}" ] && (ps -u $(whoami) -opid= |
	grep -P "^\s*$(cat ${PIDFILE})$" &> /dev/null); then
    echo "Already running."
    exit 99
fi

cd /home/zp/zeusping/for_vps/
python3 prep_for_rsync.py /home/zp/zeusping/for_vps/op_$1/ /home/zp/zeusping/for_vps/rsync_op_$1/ $1 >> ./rsync_all_but_latest.log 2>&1 &

echo $! > "${PIDFILE}"
chmod 644 "${PIDFILE}"
