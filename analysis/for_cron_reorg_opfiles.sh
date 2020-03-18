#!/bin/bash

mkdir -p "$HOME/ccaida/zeusping/analysis/tmp"
PIDFILE="$HOME/ccaida/zeusping/analysis/tmp/reorg_opfiles.pid"

if [ -e "${PIDFILE}" ] && (ps -u $(whoami) -opid= |
	grep -P "^\s*$(cat ${PIDFILE})$" &> /dev/null); then
    echo "Already running."
    exit 99
fi

cd /nmhomes/ramapad/ccaida/zeusping/analysis/
python reorg_opfiles.py /fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/op_CO_VT_RI/ /fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_CO_VT_RI/ >> ./tmp/reorg_opfiles.log 2>&1 &

echo $! > "${PIDFILE}"
chmod 644 "${PIDFILE}"
