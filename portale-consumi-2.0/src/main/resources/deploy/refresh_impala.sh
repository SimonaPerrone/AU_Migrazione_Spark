#!/bin/bash
###
# Refresh e statistiche della tabella per il motore di Impala
#
# Esecuzione:
# ./refresh_impala.sh tabella_da_elaborare nome_tabella logfile
#
# - nome_tabella: nome tabella da aggiornare
# - logfile: percorso file di log dove scrivere
###

TABLE=$1
LOGFILE=$2

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}


log "Inizio comandi Impala" | tee -a ${LOGFILE}

log "Impala comando: Invalidate metadata" | tee -a ${LOGFILE}
impala-shell -i dmphclo14 -q "INVALIDATE METADATA ${hive.table.forniture_misure_gas};" >> ${LOGFILE}

log "Impala comando: Refresh table" | tee -a ${LOGFILE}
impala-shell -i dmphclo14 -q "REFRESH ${hive.table.forniture_misure_gas};" >> ${LOGFILE}

log "Impala comando: Compute stats" | tee -a ${LOGFILE}
impala-shell -i dmphclo14 -q "COMPUTE STATS ${hive.table.forniture_misure_gas}" >> ${LOGFILE}

log "Fine comandi Impala" | tee -a ${LOGFILE}

exit 0
