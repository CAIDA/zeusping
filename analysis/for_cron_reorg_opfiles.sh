#!/bin/bash

mkdir -p "$HOME/ccaida/zeusping/analysis/tmp"
PIDFILE="$HOME/ccaida/zeusping/analysis/tmp/reorg_opfiles.pid"

if [ -e "${PIDFILE}" ] && (ps -u $(whoami) -opid= |
	grep -P "^\s*$(cat ${PIDFILE})$" &> /dev/null); then
    echo "Already running."
    exit 99
fi

cd /nmhomes/ramapad/ccaida/zeusping/analysis/
python reorg_opfiles.py op_CO_VT_RI > ./tmp/reorg_opfiles.log &

echo $! > "${PIDFILE}"
chmod 644 "${PIDFILE}"
