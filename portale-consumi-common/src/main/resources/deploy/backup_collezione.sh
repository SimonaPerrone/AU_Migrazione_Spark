#!/bin/bash
##
# Crea backup collezione MongoDB duplicandola con suffisso "_BKP".
# Se esiste gia' una collezione di backup questa viene sostituita
#
##
set -e

LOGFILE=${deploy.path.logs}/$(date "+%Y%m%d%H%M%S")-PCC-backup_collezione.txt
# File di properties del job del portale consumi gas
CONNECTION_PROPERTIES=${mongo.params.hdfs}

usage() {
    echo "Utilizzo script: $0 -c <collezione>"
    echo "-h          Mostra aiuto"
    echo "-c <String> Nome collezione di cui fare backup"
    echo "-d          Disabilita creazione file log"
}

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    if [[ $IS_LOG_DISABLED == false ]]; then
      echo "[${LOG_TIMESTAMP}] - ${1}" | tee -a "${LOGFILE}"
    else
      echo "[${LOG_TIMESTAMP}] - ${1}"
    fi
}

IS_LOG_DISABLED=false
while getopts "c:dh" opt; do
    case "${opt}" in
        c)
            COLLECTION="${OPTARG}"
            ;;
        d)
            IS_LOG_DISABLED=true
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

if [[ -z $COLLECTION ]]
then
     usage
     exit 1
fi

log "Inizio Backup"
log "Argomenti passati: $*"
log "COLLECTION: \"${COLLECTION}\""

log "File proprieta' connesione database: ${CONNECTION_PROPERTIES}"
log "Collezione target: ${COLLECTION}"

log "Lettura dati connessione database"
MONGO_URI=$(hdfs dfs -cat ${CONNECTION_PROPERTIES} | grep 'mongodb.client.uri' |  sed 's/mongodb.client.uri = //')
MONGO_DB=$(hdfs dfs -cat ${CONNECTION_PROPERTIES} | grep 'mongodb.db.name' |  sed 's/mongodb.db.name = //')
MONGO_CONN="${MONGO_URI}/${MONGO_DB}"

COLLEZIONE_BACKUP="${COLLECTION}_BKP"
log "Duplicazione collezione: ${COLLECTION} in: ${COLLEZIONE_BACKUP}"
query_backup="db.getCollection(\"${COLLECTION}\").aggregate({\$out: \"${COLLEZIONE_BACKUP}\"})"
log "$query_backup"
mongosh ${MONGO_CONN} --eval "${query_backup}"

log "Fine Backup"

exit 0
