#!/usr/bin/env bash
set -e
set -o pipefail

# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

export LOG_PATH='${logs.root.path}/sqoop-load'
export LOG_FILE_NAME="$(date '+%Y-%m-%d_%H-%M-%S')-funzionali-switching.log"
export LOG_FILE="${LOG_PATH}/${LOG_FILE_NAME}"
mkdir -p "${LOG_PATH}"
touch "${LOG_FILE}"

log() {
    local log_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${log_timestamp}] - $*"
    echo "[${log_timestamp}] - $*" >> "${LOG_FILE}"
}

die()
{
    log "$*"
    exit 2
}

needs_arg()
{
    if [ -z "$OPTARG" ];
    then
        die "No arg for --$OPT option"
    fi
}

execute_sqoop_query() {
    local query="$1"
    sqoop eval \
        --verbose \
        --connect "${JDBC_URL}" \
        --username "${JDBC_USERNAME}" \
        --password "${JDBC_PASSWORD}" \
        --query "${query}"
}

execute_sqoop_import() {
    local table="$1"
    local split_column="$2"
    sqoop import \
        --connect "${JDBC_URL}" \
        --username "${JDBC_USERNAME}" \
        --password "${JDBC_PASSWORD}" \
        --table "${table}" \
        --fields-terminated-by ';' \
        --lines-terminated-by '\n' \
        --null-string '${sqoop.null.character}' \
        --null-non-string '${sqoop.null.character}' \
        --split-by "${split_column}" \
        --hive-import \
        --hive-database "${SWITCHING_EE_HIVE_DB_NAME}" \
        --hive-table "${table}" \
        --hive-overwrite \
        --num-mappers ${sqoop.num.mappers} \
        >> "${LOG_FILE}" \
        2>&1
}

print_help()
{
    echo "ee-switching-load-funzionali-switching.sh"
    echo "                                         [-h|--help]"
    echo "                                         [-v|--verbose]"
    echo "                                         [--debug]"
    echo "                                         [-p|--pod=/path/to/file with a POD code per line]"
    echo "                                         [-f|--date-funzionali-switching=/path/to/file with a switching date per line, like 20200101]"
    echo "                                         [-d|--piva-distributore=/path/to/file with a distributor VAT number per line]"
    echo "                                         [-D|--piva-udd=/path/to/file with a supplier VAT number per line]"
    echo "                                         [-c|--coppie-piva=/path/to/file with one pair of distributor and supplier VAT numbers per line, separated by a comma]"
}

print_variables()
{
    log "Script variables:"
    log "PRINT_HELP=\"${PRINT_HELP}\""
    log "PRINT_VERBOSE=\"${PRINT_VERBOSE}\""
    log "PRINT_DEBUG=\"${PRINT_DEBUG}\""
    log "INGESTION_TIMESTAMP=\"${INGESTION_TIMESTAMP}\""
    log "PODS=\"${PODS}\""
    log "POD_ARRAY=\"${POD_ARRAY[@]}\""
    log "FUNZIONALI_SWITCHING_DATES=\"${FUNZIONALI_SWITCHING_DATES}\""
    log "FUNZIONALI_SWITCHING_ALL_MONTH=\"${FUNZIONALI_SWITCHING_ALL_MONTH}\""
    log "FUNZIONALI_SWITCHING_DATE_ARRAY=\"${FUNZIONALI_SWITCHING_DATE_ARRAY[@]}\""
    log "FUNZIONALI_SWITCHING_DATE_PARTITIONS_ARRAY=\"${FUNZIONALI_SWITCHING_DATE_PARTITIONS_ARRAY[@]}\""
    log "PIVAS_DISTR=\"${PIVAS_DISTR}\""
    log "PIVA_DISTR_ARRAY=\"${PIVA_DISTR_ARRAY[@]}\""
    log "PIVAS_UDD=\"${PIVAS_UDD}\""
    log "PIVA_UDD_ARRAY=\"${PIVA_UDD_ARRAY[@]}\""
    log "PIVAS_COUPLES=\"${PIVAS_COUPLES}\""
    log "PIVA_COUPLES_ARRAY=\"${PIVA_COUPLES_ARRAY[@]}\""
    log "SWITCHING_EE_HIVE_DB_NAME=\"${SWITCHING_EE_HIVE_DB_NAME}\""
    log "JDBC_URL=\"${JDBC_URL}\""
    log "JDBC_USERNAME=\"${JDBC_USERNAME}\""
    log ""
}

while getopts hvp:f:d:D:c:-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    h | help ) PRINT_HELP="true" ;;
    v | verbose ) PRINT_VERBOSE="true" ;;
    debug ) PRINT_DEBUG="true" ;;
    p | pod ) needs_arg; PODS="$OPTARG" ;;
    f | date-funzionali-switching ) needs_arg; FUNZIONALI_SWITCHING_DATES="$OPTARG" ;;
    d | piva-distributore ) needs_arg; PIVAS_DISTR="$OPTARG" ;;
    D | piva-udd ) needs_arg; PIVAS_UDD="$OPTARG" ;;
    c | coppie-piva ) needs_arg; PIVAS_COUPLES="$OPTARG" ;;
    ??* ) die "Illegal option --$OPT" ;;
    \? ) exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [[ ${PRINT_HELP} ]]; then
    print_help
    exit 0
fi

# load arrays
if [[ -v PODS ]]; then
    POD_ARRAY=($(cat "${PODS}" | tr '\n' ' ' | tr ',' ' '))
fi

if [[ -v FUNZIONALI_SWITCHING_DATES ]]; then
    FUNZIONALI_SWITCHING_ALL_MONTH=""
    FUNZIONALI_SWITCHING_DATE_ARRAY=($(cat "${FUNZIONALI_SWITCHING_DATES}" | tr '\n' ' '))
    for switching_date in "${FUNZIONALI_SWITCHING_DATE_ARRAY[@]}"; do
        if ! [[ "$(date -d "${switching_date}" '+%Y%m%d' 2>/dev/null)" == "${switching_date}" ]]; then
            die "Bad switching date: ${switching_date} does not have the '%Y%m%d' format."
        fi
    done
    FUNZIONALI_SWITCHING_DATE_PARTITIONS_ARRAY=($(for switching_date in "${FUNZIONALI_SWITCHING_DATE_ARRAY[@]}"; do
        echo $(date -d "${switching_date}" '+%Y%m')
    done | uniq))
else
    FUNZIONALI_SWITCHING_ALL_MONTH="true"
    FUNZIONALI_SWITCHING_DATE_ARRAY=()
    FUNZIONALI_SWITCHING_DATE_PARTITIONS_ARRAY=("$(date -d "$(date '+%Y%m')01 +1 months" '+%Y%m')")
fi

if [[ -v PIVAS_DISTR ]]; then
    PIVA_DISTR_ARRAY=($(cat "${PIVAS_DISTR}" | tr '\n' ' '))
fi

if [[ -v PIVAS_UDD ]]; then
    PIVA_UDD_ARRAY=($(cat "${PIVAS_UDD}" | tr '\n' ' '))
fi

if [[ -v PIVAS_COUPLES ]]; then
    PIVA_COUPLES_ARRAY=($(cat "${PIVAS_COUPLES}" | tr '\n' ' '))
fi

if [[ -v PODS ]]; then
    if [[ -v PIVAS_DISTR || -v PIVAS_UDD || -v PIVAS_COUPLES ]]; then
        die "Bad switch combination: if -p|--pod is set, -d, -D and -c are not accepted."
    fi
fi

if [[ -v PIVAS_COUPLES ]]; then
    if [[ -v PIVAS_DISTR || -v PIVAS_UDD || -v PODS ]]; then
        die "Bad switch combination: if -c|--coppie-piva is set, -p, -d and -D are not accepted."
    fi
fi

if [[ -v PIVAS_UDD ]]; then
    if [[ -v PIVAS_DISTR || -v PIVAS_COUPLES || -v PODS ]]; then
        die "Bad switch combination: if -D|--piva-udd is set, -p, -c and -d are not accepted."
    fi
fi

if [[ -v PIVAS_DISTR ]]; then
    if [[ -v PIVAS_UDD || -v PIVAS_COUPLES || -v PODS ]]; then
        die "Bad switch combination: if -d|--piva-distributore is set, -p, -c and -D are not accepted."
    fi
fi

# load variables from the configuration
[[ -f "${DEPLOY_PATH}/conf/hive.sh" ]] && source "${DEPLOY_PATH}/conf/hive.sh" || die "${DEPLOY_PATH}/conf/hive.sh not found"
INGESTION_TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# load the queries
source "${DEPLOY_PATH}/queries/oracle/ingestion_snippets.sql.sh"
source "${DEPLOY_PATH}/queries/hive/ingestion_snippets.hql.sh"

# load bash JDBC variables from Java properties file in HDFS
source <(${sqoop.cat.command} '${sqoop.properties.hdfs.path}' \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

if [[ "${PRINT_VERBOSE}" == "true" ]]; then
    print_variables
fi

if [[ "${PRINT_DEBUG}" != "true" ]]; then
    # drop table (ignore if not exists) and recreate temporary table
    set +e
    execute_sqoop_query "${FUNZIONALI_1_DELETE_ORACLE_QUERY}" \
        >> "${LOG_FILE}" \
        2>&1
    set -e
    execute_sqoop_query "${FUNZIONALI_1_ORACLE_QUERY}" \
        >> "${LOG_FILE}" \
        2>&1

    # check view for data if there are switches
    if [[ -v PODS || -v FUNZIONALI_SWITCHING_DATES || -v PIVAS_DISTR || -v PIVAS_UDD || -v PIVAS_COUPLES ]]; then
        FUNZIONALI_1_CHECK_RESULT=$(execute_sqoop_query "${FUNZIONALI_1_ORACLE_CHECK_QUERY}" 2>/dev/null \
            | (grep -v Accumulo || :) \
            | (grep -v -P '^-|^\|\s[A-Z]' || :))
        if [[ "${PRINT_VERBOSE}" == "true" ]]; then
            log "FUNZIONALI_1_CHECK_RESULT=\"${FUNZIONALI_1_CHECK_RESULT}\""
        fi
        if [[ "${FUNZIONALI_1_CHECK_RESULT}" == "" ]]; then
            die "The current switch combination did not generate data for the query \"${FUNZIONALI_1_ORACLE_CHECK_QUERY}\"."
        fi
    fi

    if [[ "${PRINT_VERBOSE}" == "true" ]]; then
        log "Insert the data in ${SWITCHING_EE_HIVE_DB_NAME}.${FUNZIONALI_1_ORACLE_TABLE_NAME}."
    fi

    execute_sqoop_import "${FUNZIONALI_1_ORACLE_TABLE_NAME}" "${FUNZIONALI_1_SPLIT_COLUMN}"

    if [[ "${PRINT_VERBOSE}" == "true" ]]; then
        log "Insert the data in ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_TABLE_NAME}."
    fi

    beeline \
        -u '${hive.jdbc.url}' \
        -n '${hive.jdbc.user}' \
        -e "${FUNZIONALI_1_QUERY}" \
        >> "${LOG_FILE}" \
        2>&1
else
    log "FUNZIONALI_1_DELETE_ORACLE_QUERY=${FUNZIONALI_1_DELETE_ORACLE_QUERY}"
    log "FUNZIONALI_1_ORACLE_QUERY=${FUNZIONALI_1_ORACLE_QUERY}"
    log "FUNZIONALI_1_ORACLE_CHECK_QUERY=${FUNZIONALI_1_ORACLE_CHECK_QUERY}"
    log "FUNZIONALI_1_QUERY=${FUNZIONALI_1_QUERY}"
fi

if [[ "${PRINT_VERBOSE}" == "true" ]]; then
    log "The process $(basename "${0}") finished successfully."
fi