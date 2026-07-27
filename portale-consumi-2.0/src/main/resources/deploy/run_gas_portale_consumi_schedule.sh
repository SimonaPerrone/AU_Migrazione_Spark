#!/bin/bash

## Controlla se e' primo lunedì del mese allora lancia run forzato scrittura completa altrimenti lancio standard


LOGFILE=${deploy.path.logs}/$(date "+%Y%m%d%H%M%S")-portale_consumi_gas-pianificatore.txt

GIORNO_SETTIMANA=$(date +%u)
GIORNO_MESE=$(date +%d)
LUNEDI=1

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

log "GIORNO_SETTIMANA: ${GIORNO_SETTIMANA}" | tee -a ${LOGFILE}
log "GIORNO_MESE: ${GIORNO_MESE}" | tee -a ${LOGFILE}

# Se la data di oggi e' nella prima settimana del mese (giorno mese < 8) ed e' di lunedì
if [ $GIORNO_MESE -lt 8 ] && [ $GIORNO_SETTIMANA = $LUNEDI ] ; then
    log "Oggi e' il primo lunedì del mese: lancio completo" | tee -a ${LOGFILE}
    source ${deploy.path.local}/run_gas_forniture_misure_full_forzato_portale_consumi.sh
else
    log "Lancio standard" | tee -a ${LOGFILE}
    source ${deploy.path.local}/run_gas_forniture_misure_full_portale_consumi.sh
fi
