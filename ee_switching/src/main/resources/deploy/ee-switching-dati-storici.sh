#!/usr/bin/env bash
set -e
set -o pipefail

# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

dir_compilated="${DEPLOY_PATH}/app-data/compilated"
dir_query="${DEPLOY_PATH}/app-data/utility-query"
dir_pilot="${DEPLOY_PATH}/app-data/pilot"
dir_tmp="${DEPLOY_PATH}/app-data/tmp"
dir_function="${DEPLOY_PATH}/app-data/function"
source "${dir_function}/utility.sh"

while getopts hvt:p:s:d:D:c:q:-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
  h | help) PRINT_HELP="true" ;;
  v | verbose) PRINT_VERBOSE="true" ;;
  t | path-timestamp) needs_arg; FILE_INGESTION_TIMESTAMP="$OPTARG";;
  p | path-pod-list) needs_arg; FILE_PODS="$OPTARG";;
  s | path-date-switching) needs_arg; FILE_STORICI_SWITCHING_DATES="$OPTARG";;
  d | path-piva-distributore) needs_arg; FILE_PIVAS_DISTR="$OPTARG";;
  D | path-piva-udd-list) needs_arg; FILE_PIVAS_UDD="$OPTARG";;
  c | path-pair-piva-udd-piva-distr-list) needs_arg; FILE_PAIR_PIVAS_UDD_PIVA_DISTR="$OPTARG";;
  q | queue) needs_arg; QUEUE_OPTION="-q $OPTARG" ;;
   debug) DEBUG="true";;

  ??*) die "Illegal option --$OPT" ;;
  \?) exit 2 ;;
  esac
done
#*************************************************
source "${DEPLOY_PATH}/conf/hive.sh"
#**************************************************

print_variable
convert_var_to_hive_conf_string
print_hiveconf_var
create_var
create_check_holidays "${dir_tmp}" "${dir_query}/holidays.hql" "${DEPLOY_PATH}"
create_check_switching_dates
create_check_pod
create_check_piva_distributore
create_check_udd
create_check_pair_piva_dist_piva_udd
create_added_view_purged "${dir_query}/purge_view.sh"
create_check_purge_view_sw_con_tra_view_rs "${dir_query}/purge_view_sw_con_tra_view_rs.sh"
create_map_reduce_param

print_check

compile $dir_compilated $dir_pilot

if ! [[ -v DEBUG ]];then
run -d $dir_compilated $QUEUE_OPTION
else
  log "I will not execute the run"
fi
