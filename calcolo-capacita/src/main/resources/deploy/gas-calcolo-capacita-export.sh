#!/usr/bin/env bash
set -e
set -o pipefail

# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"
[[ "${PWD}" == *"/yarn/nm"* ]] && SUB_DIR_LOG="oozie" || SUB_DIR_LOG="terminal"

source "${DEPLOY_PATH}/library/log.sh"
source "${DEPLOY_PATH}/library/sqoop.sh"
source "${DEPLOY_PATH}/library/function.sh"

export LOG_PATH="${logs.root.path}/${SUB_DIR_LOG}/export"
export LOG_FILE_NAME="$(date '+%Y-%m-%d_%H-%M-%S').log"
export LOG_FILE="${LOG_PATH}/${LOG_FILE_NAME}"
mkdir -p "${LOG_PATH}"
touch "${LOG_FILE}"


source "${DEPLOY_PATH}/query/hive/insert-in-history.sh"

load_jdbc

columns="n_id_pdr_capacita_tmp,n_id_pdr,n_id_pratica,t_codice_pdr,d_data_rif,d_data_inizio,n_anno,t_tipo_calcolo,n_pcm,n_ctc,t_processo_origine,t_esito_calcolo,t_esito_code_desc,d_data_inserimento,n_execution_id,t_esito_agg_rcu,t_errore_agg_rcu,t_esito_agg_rcu_desc,t_stato,d_data_aggiornamento"
execute_sqoop_export "${hive.table.result.db}" "${hive.table.result.name}" "${oracle.table.clg_pdr_capacita_tmp.db}" "${oracle.table.clg_pdr_capacita_tmp.name}" "$columns"  >> "${LOG_FILE}" 2>&1

beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -e "$CREATE_HISTORY" >> "${LOG_FILE}" 2>&1


