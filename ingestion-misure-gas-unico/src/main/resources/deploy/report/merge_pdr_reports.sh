#!/usr/bin/env bash
set -e

export BASE_PATH="${merge.report.base.path}"
export DEST_PATH="${merge.report.dest.path}"
export REPORT_PDR_PREFIX="ReportEsitoPDR"
export CSV_HEADER="CartellaCloud;Nomefile;PDR;Ammissibilità;Bloccante;Codice_Inammissibilità;Descrizione"
export CSV_HEADER_FILE="CartellaCloud;Nomefile;Ammissibilità;Bloccante;Codice_Inammissibilità;Descrizione"
export REPORT_FILE_STANDARD_NAME="ReportAmmissibilitàFileGAS.txt"
export REPORT_FILE_IGMG_NAME="ReportAmmissibilitàFileIGMG.txt"
export REPORT_PDR_STANDARD_NAME="ReportEsitoPdRGAS.txt"
export REPORT_PDR_IGMG_NAME="ReportEsitoPdRIGMG.txt"

die()
{
    echo "$*" >&2
    exit 2
}

needs_arg()
{
    if [ -z "$OPTARG" ];
    then
        die "No arg for --$OPT option"
    fi
}

merge_pdr_reports_day() {
    local relative_folder=$1
    local base_folder="${BASE_PATH}/${relative_folder}"
    local destination_folder="${DEST_PATH}/${relative_folder}"

    if ! [[ -d "${base_folder}" ]]; then
        return
    fi

    mkdir -p "${destination_folder}"

    if [[ -f "${base_folder}/${REPORT_FILE_IGMG_NAME}" ]]; then
        if [[ "${RECUPERO}" == "true" ]]; then
            if ! [[ -f "${destination_folder}/${REPORT_FILE_IGMG_NAME}" ]]; then
                echo "${CSV_HEADER_FILE}" > "${destination_folder}/${REPORT_FILE_IGMG_NAME}"
            fi
            sed 1d "${base_folder}/${REPORT_FILE_IGMG_NAME}" \
                >> "${destination_folder}/${REPORT_FILE_IGMG_NAME}"
        else
            cp \
                -f \
                "${base_folder}/${REPORT_FILE_IGMG_NAME}" \
                "${destination_folder}/${REPORT_FILE_IGMG_NAME}"
        fi
    fi

    if [[ -f "${base_folder}/${REPORT_FILE_STANDARD_NAME}" ]]; then
        if [[ "${RECUPERO}" == "true" ]]; then
            if ! [[ -f "${destination_folder}/${REPORT_FILE_STANDARD_NAME}" ]]; then
                echo "${CSV_HEADER_FILE}" > "${destination_folder}/${REPORT_FILE_STANDARD_NAME}"
            fi
            sed 1d "${base_folder}/${REPORT_FILE_STANDARD_NAME}" \
                >> "${destination_folder}/${REPORT_FILE_STANDARD_NAME}"
        else
            cp \
                -f \
                "${base_folder}/${REPORT_FILE_STANDARD_NAME}" \
                "${destination_folder}/${REPORT_FILE_STANDARD_NAME}"
        fi
    fi

    local igmg_num=$(find "${base_folder}" -type f -name "${REPORT_PDR_PREFIX}*IGMG*.txt" | wc -l)
    if [[ igmg_num -gt 0 ]]; then
        [[ "${RECUPERO}" == "false" ]] && echo "${CSV_HEADER}" > "${destination_folder}/${REPORT_PDR_IGMG_NAME}"
        find "${base_folder}" \
            -type f \
            -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \
            -exec sed 1d {} \; \
            2>/dev/null \
            >> "${destination_folder}/${REPORT_PDR_IGMG_NAME}"
    fi

    local std_num=$(find "${base_folder}" -type f \( -name "${REPORT_PDR_PREFIX}*.txt" -a ! -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \) | wc -l)
    if [[ std_num -gt 0 ]]; then
        [[ "${RECUPERO}" == "false" ]] && echo "${CSV_HEADER}" > "${destination_folder}/${REPORT_PDR_STANDARD_NAME}"
        find "${base_folder}" \
            -type f \
            \( -name "${REPORT_PDR_PREFIX}*.txt" -a ! -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \) \
            -exec sed 1d {} \; \
            2>/dev/null \
            >> "${destination_folder}/${REPORT_PDR_STANDARD_NAME}"
    fi
}
export -f merge_pdr_reports_day

# Handle options with getopts
while getopts d:D:R-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    d | from-date ) needs_arg; FROM_DATE="$OPTARG" ;;
    D | to-date ) needs_arg; TO_DATE="$OPTARG" ;;
    R | recovery ) RECUPERO="true" ;;
    ??* )          die "Illegal option --$OPT" ;;
    \? )           exit 2 ;;
  esac
done
shift $((OPTIND-1))

# mandatory options
if ! [ -v FROM_DATE ]
then
    die "-d DATE|--from-date=DATE (-D DATE|--to-date=DATE)"
fi
if ! [ -v TO_DATE ]
then
    TO_DATE="${FROM_DATE}"
fi
if ! [ -v RECUPERO ]
then
    RECUPERO="false"
fi
export RECUPERO

cd "${BASE_PATH}"
find . -mindepth 3 -maxdepth 3 -type d | while read sotteso; do
    if [[ "${FROM_DATE}" == "${TO_DATE}" ]]; then
        d_destination_folder=$(date -d "$FROM_DATE" '+%Y/%m%d')
        printf "%s\0" "${sotteso:2}/$d_destination_folder"
    else
        n_days=0
        d=$(date -d "$FROM_DATE + $n_days days" '+%Y-%m-%d')
        d_destination_folder=$(date -d "$FROM_DATE + $n_days days" '+%Y/%m%d')
        printf "%s\0" "${sotteso:2}/$d_destination_folder"
        until [[ "$d" == "$TO_DATE" ]]
        do
            n_days=$((n_days + 1))
            d=$(date -d "$FROM_DATE + $n_days days" '+%Y-%m-%d')
            d_destination_folder=$(date -d "$FROM_DATE + $n_days days" '+%Y/%m%d')
            printf "%s\0" "${sotteso:2}/$d_destination_folder"
        done
    fi
done | xargs -0 -n 1 -P ${merge.report.processes} bash -c 'merge_pdr_reports_day "$@"' --

