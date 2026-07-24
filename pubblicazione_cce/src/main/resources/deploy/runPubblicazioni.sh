#!/bin/bash
# data richiesta, flusso
REQUEST_DATE=$1
FLOW=$2
DATA_OGGI=$(date "+%Y%m%d%H%M%S")
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-CCE-PUBBLICAZIONE-${FLOW}.log

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

if [ -z "$1" ]
  then
    log "No request date specified. Using default:"
    REQUEST_DATE=$(date -d "$date -1 days" +"%Y-%m-%d")
fi

log "Inizio pubblicazioni CCE"

log "REQUEST_DATE: $REQUEST_DATE"

log "Import richieste"
bash ${deploy.path.local}/importRichieste.sh $REQUEST_DATE

# Inserire refresh richieste

log "Calcolo pubblicazioni $FLOW"
bash ${deploy.path.local}/runSpark.sh $REQUEST_DATE $FLOW

# Inserire refresh CA se lancio CA

log "Esportazione esiti"
bash ${deploy.path.local}/exportEsiti.sh

# Inserire refresh export

log "Fine pubblicazioni CCE"

exit 0
