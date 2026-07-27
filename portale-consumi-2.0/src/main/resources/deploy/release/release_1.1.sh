#!/bin/bash
###
# Rilascio applicativo versione 1.1
# aggiunge colonne codice offerta e vulnerabilità a tabelle FornitureGas
###
set -e

VERSIONE="V1.1"
DATA_OGGI=$(date "+%Y%m%d%H%M%S")
LOCAL_PATH=${deploy.path.local}
HDFS_PATH=${deploy.path.hdfs}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"
PATH_PROPERTIES="${LOCAL_PATH}/params.properties"
PATH_PROPERTIES_FULL="${LOCAL_PATH}/params_full.properties"
LOGFILE="${deploy.path.logs}/${DATA_OGGI}-portale_consumi_gas-rilascio_${VERSIONE}.txt"

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

log "Inizio processo rilascio $VERSIONE" | tee -a ${LOGFILE}

log "Modifica struttura Hive" | tee -a ${LOGFILE}
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${system.user} \
       -f ${PATH_HIVE_STRUCT}/ddl_forniture_processi_gas_1_1.hql

log "Caricamento file properties" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_PROPERTIES} ${HDFS_PATH}

log "Caricamento file properties full" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_PROPERTIES_FULL} ${HDFS_PATH}

log "Fine processo rilascio $VERSIONE" | tee -a ${LOGFILE}

exit 0
