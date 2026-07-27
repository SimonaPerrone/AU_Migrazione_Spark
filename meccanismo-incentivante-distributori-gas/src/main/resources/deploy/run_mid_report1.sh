#!/bin/bash
###
# Calcolo report 1.
# Richiede un parametro che rappresenta il valore della colonna executionid_daily_consumption della
# tabella mid_contatori da usare per selezionare i dati di input
#
# ./run_mid_report1.sh <executionid_daily_consumption>
###
set -e

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

DATA_OGGI=$(date "+%Y%m%d%H%M%S")
FLOW=REPORT1
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-MID-${FLOW}-log.txt

LOCAL_PATH=${deploy.path.local}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"

log "Inizio aggiornamento report 1" | tee -a ${LOGFILE}

if [[ $# -eq 0 ]] ; then
    log "Nessun parametro specificato al lancio del processo: necessario specificare valore 'EXECUTIONID_DAILY_CONSUMPTION' per la tabella '${hive.table.mid_contatori}'" | tee -a ${LOGFILE}
    log "Comando atteso: ./run_mid_report1.sh <executionid_daily_consumption>" | tee -a ${LOGFILE}
    exit 1
fi

executionId=$1
executionId_trim=${executionId// }
log "executionid_daily_consumption: \"${executionId_trim}\"" | tee -a ${LOGFILE}

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n hive \
  --hivevar executionId=${executionId_trim} \
  -f ${PATH_HIVE_STRUCT}/dml_mid_report1.hql

log "Fine aggiornamento report 1" | tee -a ${LOGFILE}
