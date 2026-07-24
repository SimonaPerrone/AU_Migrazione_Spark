#!/usr/bin/env bash
set -e

export BASE_PATH="${merge.report.base.path}"
export DEST_PATH="${merge.report.dest.path}"
export TMP_PATH="/tmp/TMP_Folder_Aggregazione_Ammissibilita_GAS"
export REPORT_PDR_PREFIX="ReportEsitoPDR"
export REPORT_FILE_STANDARD_NAME="ReportAmmissibilitàFileGAS"
export REPORT_FILE_IGMG_NAME="ReportAmmissibilitàFileIGMG"
export REPORT_PDR_STANDARD_NAME="ReportEsitoPdRGAS"
export REPORT_PDR_IGMG_NAME="ReportEsitoPdRIGMG"

die()
{
    echo "$*" >&2
    exit 2
}

merge_pdr_reports_day() {
#    set -e

    local relative_folder=$1
    local base_folder="${BASE_PATH}/${relative_folder}"
    local destination_folder="${DEST_PATH}/${relative_folder}"
    local tmp_folder="$TMP_PATH/${relative_folder}"
    local now=$(date '+%Y-%m-%d %T.%6N')
    local CURTMS=$(date -d "$now" +%Y%m%d%H%M%S)

    local anno_mesegiorno=${relative_folder: -9}
    local annomese=${anno_mesegiorno:0:4}${anno_mesegiorno:5:2}
    local annomesegiornodir=${annomese}${anno_mesegiorno:7:2}

    if ! [[ -d "${base_folder}" ]]; then
        return
    fi

    mkdir -p -m 777 "${destination_folder}"

    if [[ "${REPORT_FILE}" == "true" ]]; then

      local reports_in_input_dir_file_igmg=$(find "${base_folder}" -type f -name "${REPORT_FILE_IGMG_NAME}*.txt" | wc -l)
      if [[ reports_in_input_dir_file_igmg -gt 0 ]]; then
          local reports_in_output_dir_file_igmg=$(find "${destination_folder}" -type f -name "${REPORT_FILE_IGMG_NAME}_*.txt" | wc -l)
          if [[ reports_in_output_dir_file_igmg -eq 0 ]]; then
            sort -u ${base_folder}/${REPORT_FILE_IGMG_NAME}*.txt > "${destination_folder}/${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt"
          else
            sort -u ${destination_folder}/${REPORT_FILE_IGMG_NAME}_*.txt ${base_folder}/${REPORT_FILE_IGMG_NAME}*.txt \
                    > "${destination_folder}/${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt"
          fi

          awk -v report="${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt" -v ts="$now" -v amgdir="$annomesegiornodir" -v am="$annomese" \
                -F "\"*;\"*" 'NR > 1 {print $1","report","$2","ts","amgdir","am}' \
                "${destination_folder}/${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt" \
                >> "${REPORTS_FILE_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
      fi

      local reports_in_input_dir_file_std=$(find "${base_folder}" -type f -name "${REPORT_FILE_STANDARD_NAME}*.txt" | wc -l)
      if [[ reports_in_input_dir_file_std -gt 0 ]]; then
          local reports_in_output_dir_file_std=$(find "${destination_folder}" -type f -name "${REPORT_FILE_STANDARD_NAME}_*.txt" | wc -l)
          if [[ reports_in_output_dir_file_std -eq 0 ]]; then
            sort -u ${base_folder}/${REPORT_FILE_STANDARD_NAME}*.txt > "${destination_folder}/${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt"
          else
            sort -u ${destination_folder}/${REPORT_FILE_STANDARD_NAME}_*.txt ${base_folder}/${REPORT_FILE_STANDARD_NAME}*.txt \
                    > "${destination_folder}/${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt"
          fi

          awk -v report="${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt" -v ts="$now" -v amgdir="$annomesegiornodir" -v am="$annomese" \
                -F "\"*;\"*" 'NR > 1 {print $1","report","$2","ts","amgdir","am}' \
                "${destination_folder}/${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt" \
                >> "${REPORTS_FILE_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
      fi
    fi

    if [[ "${REPORT_PDR}" == "true" ]]; then

      local igmg_num=$(find "${base_folder}" -type f -name "${REPORT_PDR_PREFIX}*IGM*.txt" | wc -l)
      if [[ igmg_num -gt 0 ]]; then
          mkdir -p "${tmp_folder}"
          find "${base_folder}" \
                  -type f \
                  -name "${REPORT_PDR_PREFIX}*IGM*.txt" \
                  | xargs cat \
                  > "${tmp_folder}/TMP_aggregazione_IGMG.txt"
  
          local reports_in_output_dir_pdr_igmg=$(find "${destination_folder}" -type f -name "${REPORT_PDR_IGMG_NAME}_*.txt" | wc -l)
          if [[ reports_in_output_dir_pdr_igmg -eq 0 ]]; then
              sort -u "$tmp_folder/TMP_aggregazione_IGMG.txt" > "${destination_folder}/${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt"
          else
              sort -u ${destination_folder}/${REPORT_PDR_IGMG_NAME}_*.txt "${tmp_folder}/TMP_aggregazione_IGMG.txt" \
                    > "${destination_folder}/${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt"
          fi

          awk -v report="${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt" -v ts="$now" -v amgdir="$annomesegiornodir" -v am="$annomese" \
                -F "\"*;\"*" 'NR > 1 {print $1","report","$2","$3","ts","amgdir","am}' \
                "${destination_folder}/${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt" \
                >> "${REPORTS_PDR_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
      fi

      local std_num=$(find "${base_folder}" -type f \( -name "${REPORT_PDR_PREFIX}*.txt" -a ! -name "${REPORT_PDR_PREFIX}*IGM*.txt" \) | wc -l)
      if [[ std_num -gt 0 ]]; then
          mkdir -p "${tmp_folder}"
          find "${base_folder}" \
                  -type f \
                  \( -name "${REPORT_PDR_PREFIX}*.txt" -a ! -name "${REPORT_PDR_PREFIX}*IGM*.txt" \) \
                  | xargs cat \
                  > "${tmp_folder}/TMP_Aggregazione_STD.txt"
  
          local reports_in_output_dir_pdr_std=$(find "${destination_folder}" -type f -name "${REPORT_PDR_STANDARD_NAME}_*.txt" | wc -l)
          if [[ reports_in_output_dir_pdr_std -eq 0 ]]; then
              sort -u "$tmp_folder/TMP_Aggregazione_STD.txt" > "${destination_folder}/${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt"
          else
              sort -u ${destination_folder}/${REPORT_PDR_STANDARD_NAME}_*.txt "${tmp_folder}/TMP_Aggregazione_STD.txt" \
                    > "${destination_folder}/${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt"
          fi

          awk -v report="${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt" -v ts="$now" -v amgdir="$annomesegiornodir" -v am="$annomese" \
                -F "\"*;\"*" 'NR > 1 {print $1","report","$2","$3","ts","amgdir","am}' \
                "${destination_folder}/${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt" \
                >> "${REPORTS_PDR_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
      fi
    fi
}
export -f merge_pdr_reports_day


while getopts fp-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
#    OPTARG="${OPTARG#$OPT}"
#    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    f | file ) REPORT_FILE="true" ;;
    p | pdr  ) REPORT_PDR="true" ;;
    ??* )      die "Illegal option --$OPT" ;;
    \? )       exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [[ ( (! -v REPORT_FILE) && (! -v REPORT_PDR) ) ]]; then
  die "-f|--f|--file / -p|--p|--pdr"
fi

if [[ ! -v REPORT_FILE ]]; then
  REPORT_FILE="false"
fi

if [[ ! -v REPORT_PDR ]]; then
  REPORT_PDR="false"
fi

export REPORT_FILE
export REPORT_PDR

if [[ ! -v REPORTS_FILE_LIST_CSV_FOLDER ]]; then
  export REPORTS_FILE_LIST_CSV_FOLDER="${merge.report.tracking.folder}/file"
fi

if [[ ! -v REPORTS_PDR_LIST_CSV_FOLDER ]]; then
  export REPORTS_PDR_LIST_CSV_FOLDER="${merge.report.tracking.folder}/pdr"
fi

mkdir -p -m 777 ${REPORTS_FILE_LIST_CSV_FOLDER}
mkdir -p -m 777 ${REPORTS_PDR_LIST_CSV_FOLDER}

rm -rf ${TMP_PATH} # pre-pulizia di sicurezza
mkdir -p ${TMP_PATH}

cd "${BASE_PATH}"
find . -mindepth 5 -maxdepth 5 -type d | while read sotteso; do

    printf "%s\0" "${sotteso:2}"

done | xargs -0 -n 1 -P ${merge.report.processes} bash -c 'merge_pdr_reports_day "$@"' --

rm -rf ${TMP_PATH}
