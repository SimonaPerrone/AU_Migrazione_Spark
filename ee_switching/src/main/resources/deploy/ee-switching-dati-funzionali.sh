#!/usr/bin/env bash
set -e
set -o pipefail

# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

export LOG_PATH='${logs.root.path}/calcolo-funzionali'
export LOG_FILE_NAME="$(date '+%Y-%m-%d_%H-%M-%S').log"
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

print_help()
{
    echo "ee-switching-dati-funzionali.sh"
    echo "                                [-h|--help]"
    echo "                                [-v|--verbose]"
    echo "                                [--debug]"
    echo "                                [-t|--timestamp=/path/to/file with a timestamp in one line, like \"\$(date '+%Y-%m-%d %H:%M:%S')\"]"
    echo "                                [-p|--pod=/path/to/file with a POD code per line]"
    echo "                                [-f|--date-funzionali-switching=/path/to/file with a switching date per line, like 20200101]"
    echo "                                [-F|--date-funzionali-na=/path/to/file with a new activation date per line, like 20200101]"
    echo "                                [-d|--piva-distributore=/path/to/file with a distributor VAT number per line]"
    echo "                                [-D|--piva-udd=/path/to/file with a supplier VAT number per line]"
    echo "                                [-c|--coppie-piva=/path/to/file with one pair of distributor and supplier VAT numbers per line, separated by a comma]"
}

print_variables()
{
    log "Script variables:"
    log "PRINT_HELP=\"${PRINT_HELP}\""
    log "PRINT_VERBOSE=\"${PRINT_VERBOSE}\""
    log "PRINT_DEBUG=\"${PRINT_DEBUG}\""
    log "INGESTION_TIMESTAMP_FILE=\"${INGESTION_TIMESTAMP_FILE}\""
    log "INGESTION_TIMESTAMP=\"${INGESTION_TIMESTAMP}\""
    log "LOADING_TIMESTAMP=\"${LOADING_TIMESTAMP}\""
    log "PODS=\"${PODS}\""
    log "POD_ARRAY=\"${POD_ARRAY[@]}\""
    log "SWITCHING_DATES=\"${SWITCHING_DATES}\""
    log "NO_SWITCHING_DATES=\"${NO_SWITCHING_DATES}\""
    log "SWITCHING_DATE_ARRAY=\"${SWITCHING_DATE_ARRAY[@]}\""
    log "SWITCHING_DATE_PARTITIONS_ARRAY=\"${SWITCHING_DATE_PARTITIONS_ARRAY[@]}\""
    log "NEW_ACTIVATION_DATES=\"${NEW_ACTIVATION_DATES}\""
    log "NO_NEW_ACTIVATION_DATES=\"${NO_NEW_ACTIVATION_DATES}\""
    log "NEW_ACTIVATION_DATE_ARRAY=\"${NEW_ACTIVATION_DATE_ARRAY[@]}\""
    log "NEW_ACTIVATION_DATE_PARTITIONS_ARRAY=\"${NEW_ACTIVATION_DATE_PARTITIONS_ARRAY[@]}\""
    log "ALL_DATES_PARTITIONS_ARRAY=\"${ALL_DATES_PARTITIONS_ARRAY[@]}\""
    log "PIVAS_DISTR=\"${PIVAS_DISTR}\""
    log "PIVA_DISTR_ARRAY=\"${PIVA_DISTR_ARRAY[@]}\""
    log "PIVAS_UDD=\"${PIVAS_UDD}\""
    log "PIVA_UDD_ARRAY=\"${PIVA_UDD_ARRAY[@]}\""
    log "PIVAS_COUPLES=\"${PIVAS_COUPLES}\""
    log "PIVA_COUPLES_ARRAY=\"${PIVA_COUPLES_ARRAY[@]}\""
    log "SWITCHING_EE_HIVE_DB_NAME=\"${SWITCHING_EE_HIVE_DB_NAME}\""
    log "SWITCHING_EE_HIVE_FUNZIONALI_TABLE_NAME=\"${SWITCHING_EE_HIVE_FUNZIONALI_TABLE_NAME}\""
    log "SWITCHING_EE_HIVE_FUNZIONALI_NA_TABLE_NAME=\"${SWITCHING_EE_HIVE_FUNZIONALI_NA_TABLE_NAME}\""
    log "SWITCHING_EE_HIVE_FUNZIONALI_OUTPUT_TABLE_NAME=\"${SWITCHING_EE_HIVE_FUNZIONALI_OUTPUT_TABLE_NAME}\""
    log ""
}

while getopts hvt:p:f:F:d:D:c:-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    h | help ) PRINT_HELP="true" ;;
    v | verbose ) PRINT_VERBOSE="true" ;;
    debug ) PRINT_DEBUG="true" ;;
    t | timestamp ) needs_arg; INGESTION_TIMESTAMP_FILE="$OPTARG" ;;
    p | pod ) needs_arg; PODS="$OPTARG" ;;
    f | date-funzionali-switching ) needs_arg; SWITCHING_DATES="$OPTARG" ;;
    F | date-funzionali-na ) needs_arg; NEW_ACTIVATION_DATES="$OPTARG" ;;
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

if [[ -v SWITCHING_DATES ]]; then
    SWITCHING_DATE_ARRAY=($(cat "${SWITCHING_DATES}" | tr '\n' ' ' | tr ',' ' '))
    for switching_date in "${SWITCHING_DATE_ARRAY[@]}"; do
        if ! [[ "$(date -d "${switching_date}" '+%Y%m%d' 2>/dev/null)" == "${switching_date}" ]]; then
            die "Bad switching date: ${switching_date} does not have the '%Y%m%d' format."
        fi
    done
    SWITCHING_DATE_PARTITIONS_ARRAY=($(for switching_date in "${SWITCHING_DATE_ARRAY[@]}"; do
        echo $(date -d "${switching_date}" '+%Y%m')
    done | sort | uniq))
else
    SWITCHING_DATE_ARRAY=()
    SWITCHING_DATE_PARTITIONS_ARRAY=("$(date -d "$(date '+%Y%m')01 +1 months" '+%Y%m')")
fi

if [[ -v NEW_ACTIVATION_DATES ]]; then
    NEW_ACTIVATION_DATE_ARRAY=($(cat "${NEW_ACTIVATION_DATES}" | tr '\n' ' ' | tr ',' ' '))
    for new_activation_date in "${NEW_ACTIVATION_DATE_ARRAY[@]}"; do
        if ! [[ "$(date -d "${new_activation_date}" '+%Y%m%d' 2>/dev/null)" == "${new_activation_date}" ]]; then
            die "Bad new activation date: ${new_activation_date} does not have the '%Y%m%d' format."
        fi
    done
    NEW_ACTIVATION_DATE_PARTITIONS_ARRAY=($(for switching_date in "${NEW_ACTIVATION_DATE_ARRAY[@]}"; do
        echo $(date -d "${switching_date}" '+%Y%m')
    done | sort | uniq))
else
    NEW_ACTIVATION_DATE_ARRAY=()
    NEW_ACTIVATION_DATE_PARTITIONS_ARRAY=("$(date '+%Y%m')")
fi

ALL_DATES_PARTITIONS_ARRAY=($(for switching_date in "${SWITCHING_DATE_PARTITIONS_ARRAY[@]}" "${NEW_ACTIVATION_DATE_PARTITIONS_ARRAY[@]}"; do
        echo "${switching_date}"
    done | sort | uniq))

if [[ -v PIVAS_DISTR ]]; then
    PIVA_DISTR_ARRAY=($(cat "${PIVAS_DISTR}" | tr '\n' ' ' | tr ',' ' '))
fi

if [[ -v PIVAS_UDD ]]; then
    PIVA_UDD_ARRAY=($(cat "${PIVAS_UDD}" | tr '\n' ' ' | tr ',' ' '))
fi

if [[ -v PIVAS_COUPLES ]]; then
    PIVA_COUPLES_ARRAY=($(cat "${PIVAS_COUPLES}" | tr '\n' ' '))
fi

if [[ -v INGESTION_TIMESTAMP_FILE ]]; then
    if [[ -v PODS || -v SWITCHING_DATES || -v NEW_ACTIVATION_DATES || -v PIVAS_DISTR || -v PIVAS_UDD || -v PIVAS_COUPLES ]]; then
        die "Bad switch combination: if -t|--timestamp is set, it is the only filtering switch accepted."
    fi

    INGESTION_TIMESTAMP=$(cat "${INGESTION_TIMESTAMP_FILE}")
    if ! [[ "$(date -d "${INGESTION_TIMESTAMP}" '+%Y-%m-%d %H:%M:%S' 2>/dev/null)" == "${INGESTION_TIMESTAMP}" ]]; then
        die "Bad ingestion timestamp: '${INGESTION_TIMESTAMP}' does not have the '+%Y-%m-%d %H:%M:%S' format."
    fi
    SWITCHING_DATE_PARTITIONS_ARRAY=()
    NEW_ACTIVATION_DATE_PARTITIONS_ARRAY=()
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

if [[ -v NEW_ACTIVATION_DATES ]]; then
    if ! [[ -v SWITCHING_DATES ]]; then
        NO_SWITCHING_DATES="true"
    fi
fi

if [[ -v SWITCHING_DATES ]]; then
    if ! [[ -v NEW_ACTIVATION_DATES ]]; then
        NO_NEW_ACTIVATION_DATES="true"
    fi
fi

# load variables from the configuration
[[ -f "${DEPLOY_PATH}/conf/hive.sh" ]] && source "${DEPLOY_PATH}/conf/hive.sh" || die "${DEPLOY_PATH}/conf/hive.sh not found"
LOADING_TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# load the queries
source "${DEPLOY_PATH}/queries/hive/funzionali_snippets.hql.sh"

if [[ "${PRINT_VERBOSE}" == "true" ]]; then
    print_variables
fi

if [[ "${PRINT_DEBUG}" != "true" ]]; then
    CHECK_RESULT=$(beeline \
        -u '${hive.jdbc.url}' \
        -n '${hive.jdbc.user}' \
        --outputformat=csv2 \
        --showHeader=false \
        -e "${FUNZIONALI_ALL_CHECK_QUERY}" \
        2>>"${LOG_FILE}")
    if [[ "${PRINT_VERBOSE}" == "true" ]]; then
        log "CHECK_RESULT=\"${CHECK_RESULT}\""
    fi
    if [[ "${CHECK_RESULT}" == "" ]]; then
        die "The current switch combination did not generate data."
    fi

    if [[ "${PRINT_VERBOSE}" == "true" ]]; then
        log "The current switch combination generated at least one record, so insert the data in ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_OUTPUT_TABLE_NAME}"
    fi

    beeline \
        -u '${hive.jdbc.url}' \
        -n '${hive.jdbc.user}' \
        -e "${FUNZIONALI_ALL_INSERT_QUERY}" \
        >> "${LOG_FILE}" \
        2>&1

    if [[ "${PRINT_VERBOSE}" == "true" ]]; then
        log "Insert bad data in ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_SCARTI_TABLE_NAME}"
    fi

    beeline \
        -u '${hive.jdbc.url}' \
        -n '${hive.jdbc.user}' \
        -e "${FUNZIONALI_ALL_SCARTI_QUERY}" \
        >> "${LOG_FILE}" \
        2>&1
else
    log "FUNZIONALI_ALL_CHECK_QUERY=${FUNZIONALI_ALL_CHECK_QUERY}"
    log "FUNZIONALI_ALL_INSERT_QUERY=${FUNZIONALI_ALL_INSERT_QUERY}"
    log "FUNZIONALI_ALL_SCARTI_QUERY=${FUNZIONALI_ALL_SCARTI_QUERY}"
fi

if [[ "${PRINT_VERBOSE}" == "true" ]]; then
    log "The process $(basename "${0}") finished successfully."
fi