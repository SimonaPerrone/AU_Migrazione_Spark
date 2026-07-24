#!/bin/bash
###
# Rilascio applicativo calcolo annuale versione 1.0
###
set -e

DATA_OGGI=$(date "+%Y%m%d%H%M%S")
REAL_PATH="$(dirname "$(realpath "${0}")")"
SCRIPTS_PATH="${REAL_PATH}/.."
HDFS_PATH=${hdfs.deploy.path}
PATH_HIVE_STRUCT="${SCRIPTS_PATH}/hive"
PATH_SQOOP="${SCRIPTS_PATH}/sqoop"
PATH_PROPERTIES="${SCRIPTS_PATH}/params.properties"
LOGFILE=${SCRIPTS_PATH}/logs/${DATA_OGGI}-gse-calcolo-annuale-rilascio_v.1.0.txt

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}
echo "Creazione cartella logs"
mkdir -p ${SCRIPTS_PATH}/logs

log "Inizio processo rilascio" | tee -a ${LOGFILE}

log "Impostazione permessi file: ${SCRIPTS_PATH}, ${PATH_SQOOP}" | tee -a ${LOGFILE}
chmod 755 ${SCRIPTS_PATH}/*
chmod 755 ${PATH_SQOOP}/*

log "Creazione strutture Hive: $PATH_HIVE_STRUCT" | tee -a ${LOGFILE}
hive -f ${PATH_HIVE_STRUCT}/create-gse-aggr-a.hql

hive -f ${PATH_HIVE_STRUCT}/create-gse-aggr-a-export.hql

hive -f ${PATH_HIVE_STRUCT}/create-gse-bi-pod-one.hql

log "Creazione cartella properties" | tee -a ${LOGFILE}
hdfs dfs -mkdir -p ${HDFS_PATH}

log "Caricamento file properties" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_PROPERTIES} ${HDFS_PATH}

log "Fine processo rilascio" | tee -a ${LOGFILE}

exit 0
