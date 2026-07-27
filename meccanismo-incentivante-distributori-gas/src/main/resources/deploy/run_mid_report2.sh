#!/bin/bash
###
# Calcolo report 2
#
# ./run_mid_report2.sh
###
set -e

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

DATA_OGGI=$(date "+%Y%m%d%H%M%S")
FLOW=REPORT2
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-MID-${FLOW}-log.txt

LOCAL_PATH=${deploy.path.local}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"

log "Inizio aggiornamento report 2" | tee -a ${LOGFILE}

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n hive \
  -f ${PATH_HIVE_STRUCT}/dml_mid_report2.hql

log "Fine aggiornamento report 2" | tee -a ${LOGFILE}
