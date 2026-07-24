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
export LOG_FILE_NAME="gas-calcolo-capacita-import-perimetro-pdr-$(date '+%Y-%m-%d_%H-%M-%S').log"
export LOG_FILE="${LOG_PATH}/${LOG_FILE_NAME}"
mkdir -p "${LOG_PATH}"
touch "${LOG_FILE}"

catch_getopts_import "$@"

create_var_import

load_jdbc

dir="/user/${hdfs.user}/${hive.table.perimetropdr.db}.${hive.table.perimetropdr.name}"

execute_sqoop_import "${oracle.table.perimetropdr.name.db}" \
                     "${oracle.table.perimetropdr.name}" \
                     "${dir}" >> "${LOG_FILE}" 2>&1

export CREATE_TABLE=$(cat <<-EOF
DROP TABLE IF EXISTS ${hive.table.perimetropdr.db}.${hive.table.perimetropdr.name};
CREATE EXTERNAL TABLE ${hive.table.perimetropdr.db}.${hive.table.perimetropdr.name}(
   N_ID_PDR string,
   T_CODICE_PDR string,
   T_VALIDO string

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${dir}'
EOF
)

beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -e "$CREATE_TABLE" >> "${LOG_FILE}" 2>&1

