#!/bin/bash
###
# Rilascio applicativo versione 1.0
###
set -e

DATA_OGGI=$(date "+%Y%m%d%H%M%S")
LOCAL_PATH=${deploy.path.local}
HDFS_PATH=${deploy.path.hdfs}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"
PATH_PROPERTIES="${LOCAL_PATH}/params.properties"
PATH_PROPERTIES_FULL="${LOCAL_PATH}/params_full.properties"
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-portale_consumi_gas-rilascio_v.1.0.txt

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}
echo "Creazione cartella logs"
mkdir -p ${deploy.path.logs}

log "Inizio processo rilascio" | tee -a ${LOGFILE}

log "Impostazione permessi file" | tee -a ${LOGFILE}
chmod 755 ${LOCAL_PATH}/*

log "Creazione strutture Hive" | tee -a ${LOGFILE}
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${system.user} \
       -f ${PATH_HIVE_STRUCT}/ddl_forniture_misure_gas.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${system.user} \
       -f ${PATH_HIVE_STRUCT}/ddl_forniture_processi_gas.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${system.user} \
       -f ${PATH_HIVE_STRUCT}/ddl_misure_storic_f2.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${system.user} \
       -f ${PATH_HIVE_STRUCT}/ddl_misure_data_calcolo.hql

log "Creazione cartella properties" | tee -a ${LOGFILE}
hdfs dfs -mkdir -p ${HDFS_PATH}

log "Caricamento file properties" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_PROPERTIES} ${HDFS_PATH}

log "Caricamento file properties full" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_PROPERTIES_FULL} ${HDFS_PATH}

log "Fine processo rilascio" | tee -a ${LOGFILE}

exit 0
