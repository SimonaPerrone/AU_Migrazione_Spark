#!/usr/bin/env bash
set -e
set -o pipefail

# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"
[[ "${PWD}" == *"/yarn/nm"* ]] && SUB_DIR_LOG="oozie" || SUB_DIR_LOG="terminal"

source "${DEPLOY_PATH}/library/log.sh"
source "${DEPLOY_PATH}/library/sqoop.sh"
source "${DEPLOY_PATH}/library/function.sh"

export LOG_PATH="${logs.root.path}/${SUB_DIR_LOG}/import"
export LOG_FILE_NAME="gas-calcolo-capacita-import-run-insert-anagrafica-$(date '+%Y-%m-%d_%H-%M-%S').log"
export LOG_FILE="${LOG_PATH}/${LOG_FILE_NAME}"
mkdir -p "${LOG_PATH}"
touch "${LOG_FILE}"

source "${DEPLOY_PATH}/query/hive/insert-in-anagrafica.sh"

load_jdbc

beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -e "$CREATE_ANAGRAFICA" >> "${LOG_FILE}" 2>&1

