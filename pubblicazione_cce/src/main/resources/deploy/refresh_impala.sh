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


log "Inizio comandi Impala"

log "Impala comando: Invalidate metadata"
impala-shell -i dmphclo14 -q "INVALIDATE METADATA $TABLE;"

log "Impala comando: Refresh table"
impala-shell -i dmphclo14 -q "REFRESH $TABLE;"

log "Impala comando: Compute stats"
impala-shell -i dmphclo14 -q "COMPUTE STATS $TABLE"

log "Fine comandi Impala"

exit 0
