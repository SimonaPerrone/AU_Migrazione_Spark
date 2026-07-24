#!/bin/sh
set -e

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties
DATE=$(date +'%Y-%m-%d_%H:%M:%S')
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

while getopts ":p:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}" ;;
    \?) echo "Invalid option -$OPTARG" >&2 ;;
  esac
done

log "Running GSE Energy Release - Calcolo Annuale..."

log "PROPERTIES_PATH: $PROPERTIES_PATH"
log "DEPLOY_PATH: $DEPLOY_PATH"

log "Importing GSE_PERIMETRO_ER_EE..."
$DEPLOY_PATH/sqoop/sqoop-import-gse-perimeter.sh

log "Importing GSE_RICHIESTA_ER_A..."
$DEPLOY_PATH/sqoop/sqoop-import-gse-yearly-requests.sh

log "Starting Spark process..."
$DEPLOY_PATH/spark-submit-gse-calcolo-annuale.sh -p $PROPERTIES_PATH 

log "Exporting GSE_AGGR_A_EXPORT..."
$DEPLOY_PATH/sqoop/sqoop-export-gse-aggr-a.sh

log "GSE energy release workflow ended successfully."
