#!/bin/bash
###
# Carica la tabella mid contatori con i dati di storico presenti nelle tabelle
# atg_bis.pdr_inc_exc_sbg_mid
# atg_bis.pdr_inc_gdm_sbg_mid
###
set -e

DATA_OGGI=$(date "+%Y%m%d%H%M%S")
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-MID-caricamento_storico_contatori.txt
LOCAL_PATH=${deploy.path.local}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

###

log "Caricamento contatori storico" | tee -a ${LOGFILE}

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n hive \
  -f ${PATH_HIVE_STRUCT}/dml_mid_contatori_storico.hql

log "Fine caricamento contatori storico" | tee -a ${LOGFILE}

exit 0
