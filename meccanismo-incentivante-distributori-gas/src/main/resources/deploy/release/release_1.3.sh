#!/bin/bash
###
# Rilascio applicativo versione 1.3, modifiche al tracciato MID2
###
set -e

DATA_OGGI=$(date "+%Y%m%d%H%M%S")

LOCAL_PATH=${deploy.path.local}
HDFS_PATH=${deploy.path.hdfs}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"
PATH_PROPERTIES="${LOCAL_PATH}/params"


LOGFILE=${deploy.path.logs}/${DATA_OGGI}-MID-rilascio_v.1.3.txt

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

###
log "Inizio processo rilascio" | tee -a ${LOGFILE}

log "Aggiornamento tabella Hive mid2" | tee -a ${LOGFILE}
# aggiornamento tabella Mid2 dettaglio
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n ${system.user} \
  -f ${PATH_HIVE_STRUCT}/ddl_mid2_dettaglio_1.3.hql

log "Aggiornamento file properties" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_PROPERTIES}/params_mid2.properties ${HDFS_PATH}

log "Fine processo rilascio" | tee -a ${LOGFILE}

exit 0
