#!/bin/bash
##
# Elimina dati collezioni Audit antecedenti a due anni fa
#
# ./pulizia_dati_audit.sh -m <mesi_nel_passato>
# <mesi_nel_passato> <Intero>(default: 24):
# utilizzato per calcolare la data soglia entro la quale cancellare i dati
# data attuale - mesi_nel_passato
##
set -e

# Funzione per mostrare l'uso corretto dello script
usage() {
    echo "Utilizzo: $0 [-m <int>(default:24)]"
}

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

MESI_NEL_PASSATO=24
MESI_NEL_PASSATO_SOGLIA_MINIMA=24
while getopts "m:h" opt; do
    case "${opt}" in
        m)
            MESI_NEL_PASSATO=${OPTARG}
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 1
            ;;
    esac
done

if [[ "$MESI_NEL_PASSATO" -lt "$MESI_NEL_PASSATO_SOGLIA_MINIMA" ]];then
  echo "Parametro -m ($MESI_NEL_PASSATO) minore della soglia minima ($MESI_NEL_PASSATO_SOGLIA_MINIMA)"
  exit 1
fi

LOGFILE=${deploy.path.logs}/$(date "+%Y%m%d%H%M%S")-PCC-audit-clean.txt
# File di properties del job del portale consumi gas
CONNECTION_PROPERTIES=${mongo.params.hdfs}
AUDIT_ELETTRICO_COLLECTION=${mongo.table.audit_elettrico}
AUDIT_GAS_COLLECTION=${mongo.table.audit_gas}

log "Inizio pulizia dati Audit" | tee -a ${LOGFILE}
echo "Argomenti passati: $*" | tee -a ${LOGFILE}

### LETTURA PROPRIETA'

log "File proprieta' connesione database: ${CONNECTION_PROPERTIES}" | tee -a ${LOGFILE}

DATA_SOGLIA=$(date -d "$MESI_NEL_PASSATO months ago" +"%Y-%m-%d")

log "Mesi nel passato: ${MESI_NEL_PASSATO}" | tee -a ${LOGFILE}
log "Data soglia pulizia: ${DATA_SOGLIA}" | tee -a ${LOGFILE}

log "Lettura dati connessione database" | tee -a ${LOGFILE}
MONGO_URI=$(hdfs dfs -cat ${CONNECTION_PROPERTIES} | grep 'mongodb.client.uri' |  sed 's/mongodb.client.uri = //')
MONGO_DB=$(hdfs dfs -cat ${CONNECTION_PROPERTIES} | grep 'mongodb.db.name' |  sed 's/mongodb.db.name = //')
MONGO_CONN="${MONGO_URI}/${MONGO_DB}"

### EE

log "Backup dati Audit Elettrico: ${AUDIT_ELETTRICO_COLLECTION}" | tee -a ${LOGFILE}
bash ${deploy.path.local}/backup_collezione.sh -d -c ${AUDIT_ELETTRICO_COLLECTION} | tee -a ${LOGFILE}

log "Pulizia dati Audit Elettrico: ${AUDIT_ELETTRICO_COLLECTION}" | tee -a ${LOGFILE}
query_elettrico="db.getCollection(\"${AUDIT_ELETTRICO_COLLECTION}\").updateMany({ }, {\$pull: {audit: {data: {\$lt: \"${DATA_SOGLIA}\"} } }}, {multi: true})"
log "$query_elettrico" | tee -a ${LOGFILE}
mongosh ${MONGO_CONN} --eval "${query_elettrico}" | tee -a ${LOGFILE}

### GAS

log "Backup dati Audit Gas: ${AUDIT_GAS_COLLECTION}" | tee -a ${LOGFILE}
bash ${deploy.path.local}/backup_collezione.sh -d -c ${AUDIT_GAS_COLLECTION} | tee -a ${LOGFILE}

log "Pulizia dati Audit Gas: ${AUDIT_GAS_COLLECTION}" | tee -a ${LOGFILE}
query_gas="db.getCollection(\"${AUDIT_GAS_COLLECTION}\").updateMany({ }, {\$pull: {audit: {data: {\$lt: \"${DATA_SOGLIA}\"} } }}, {multi: true})"
log "$query_gas" | tee -a ${LOGFILE}
mongosh ${MONGO_CONN} --eval "${query_gas}" | tee -a ${LOGFILE}

log "Fine pulizia dati Audit" | tee -a ${LOGFILE}

exit 0
