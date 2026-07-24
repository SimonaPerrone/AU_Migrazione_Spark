#!/bin/sh
# Procedura di preparazione
#
set -e

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

YEAR=$1
DATE=$(date +'%Y-%m-%d_%H:%M:%S')

log "Running GSE Energy Release - Preparazione..."

[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"
log "DEPLOY_PATH: $DEPLOY_PATH"

if [[ -z YEAR ]] || [[ "$YEAR" == "" ]]; then
  log "Parameter YEAR not set: using default (current year - 1)"
  YEAR=$(date +%Y -d'1 year ago')
fi
log "YEAR set to: $YEAR"

log "Importing ${gse.db}.POD_ONE (year: $YEAR)"
sh $DEPLOY_PATH/sqoop/sqoop-import-pod-gse-perimeter.sh $YEAR
log "${gse.db}.POD_ONE imported successfully"

log "Running main_one procedure (${sh.mainone.path}/run_mainone.sh)..."
for MONTH in $(seq 1 12);
do
  log "Running: run_mainone $YEAR $MONTH"
  sh ${sh.mainone.path}/run_mainone.sh $YEAR $MONTH
done
log "Main_one procedure completed"

log "GSE Energy Release - Preparazione, completed"
