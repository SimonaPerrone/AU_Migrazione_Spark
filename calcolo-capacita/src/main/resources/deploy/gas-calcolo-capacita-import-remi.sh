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
export LOG_FILE_NAME="gas-calcolo-capacita-import-remi-$(date '+%Y-%m-%d_%H-%M-%S').log"
export LOG_FILE="${LOG_PATH}/${LOG_FILE_NAME}"
mkdir -p "${LOG_PATH}"
touch "${LOG_FILE}"

catch_getopts_import "$@"

create_var_import

source "${DEPLOY_PATH}/query/oracle/${oracle.query.create_view_remi}"

load_jdbc

execute_sqoop_query "$CREATE_VIEW_REMI"  >> "${LOG_FILE}" 2>&1

dir="/user/${hdfs.user}/${hive.table.clg_perimetro_remi_gm_view.db}.${hive.table.clg_perimetro_remi_gm_view.name}"


execute_sqoop_import "${oracle.table.clg_perimetro_remi_gm_view.db}" \
                     "${oracle.table.clg_perimetro_remi_gm_view.name}" \
                     "${dir}" >> "${LOG_FILE}" 2>&1

export CREATE_TABLE=$(cat <<-EOF
DROP TABLE IF EXISTS ${hive.table.clg_perimetro_remi_gm_view.db}.${hive.table.clg_perimetro_remi_gm_view.name};
CREATE EXTERNAL TABLE ${hive.table.clg_perimetro_remi_gm_view.db}.${hive.table.clg_perimetro_remi_gm_view.name}(


   DATA_CALC string,
   ANNO string,
   N_ID_REMI_ANAGRAFICA string,
   T_REMI string,
   T_Z string,
   T_PMAX string,
   T_Cod_Profilo string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${dir}'
EOF
)

beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -e "$CREATE_TABLE" >> "${LOG_FILE}" 2>&1
