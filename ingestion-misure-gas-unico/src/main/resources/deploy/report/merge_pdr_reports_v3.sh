#!/usr/bin/env bash
set -e

export BASE_PATH="${merge.report.base.path}"
export DEST_PATH="${merge.report.dest.path}"
export TMP_PATH="/tmp/TMP_Folder_Aggregazione_Ammissibilita_GAS"
export REPORT_PDR_PREFIX="ReportEsitoPDR"
#export CSV_HEADER_PDR="CartellaCloud;Nomefile;PDR;Ammissibilità;Bloccante;Codice_Inammissibilità;Descrizione"
#export CSV_HEADER_FILE="CartellaCloud;Nomefile;Ammissibilità;Bloccante;Codice_Inammissibilità;Descrizione"
export REPORT_FILE_STANDARD_NAME="ReportAmmissibilitàFileGAS"
export REPORT_FILE_IGMG_NAME="ReportAmmissibilitàFileIGMG"
export REPORT_PDR_STANDARD_NAME="ReportEsitoPdRGAS"
export REPORT_PDR_IGMG_NAME="ReportEsitoPdRIGMG"

die()
{
    echo "$*" >&2
    exit 2
}

#needs_arg()
#{
#    if [ -z "$OPTARG" ];
#    then
#        die "No arg for --$OPT option"
#    fi
#}

merge_pdr_reports_day() {
    local relative_folder=$1
    local base_folder="${BASE_PATH}/${relative_folder}"
    local destination_folder="${DEST_PATH}/${relative_folder}"
    local tmp_folder="$TMP_PATH/${relative_folder}"
    local CURTMS=$(date +%Y%m%d%H%M%S)

    if ! [[ -d "${base_folder}" ]]; then
        return
    fi

    mkdir -p -m 777 "${destination_folder}"

    if [[ "${REPORT_FILE}" == "true" ]]; then

      local reports_in_input_dir_file_igmg=$(find "${base_folder}" -type f -name "${REPORT_FILE_IGMG_NAME}*.txt" | wc -l)
      if [[ reports_in_input_dir_file_igmg -gt 0 ]]; then
          local reports_in_output_dir_file_igmg=$(find "${destination_folder}" -type f -name "${REPORT_FILE_IGMG_NAME}_*.txt" | wc -l)
#          local reports_in_output_dir_file_igmg=$(ls -dq ${destination_folder}/${REPORT_FILE_IGMG_NAME}_*.txt | wc -l)
          if [[ reports_in_output_dir_file_igmg -eq 0 ]]; then
#            cp -f \
#               "${base_folder}/${REPORT_FILE_IGMG_NAME}.txt" \
#               "${destination_folder}/${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt"
            sort -u ${base_folder}/${REPORT_FILE_IGMG_NAME}*.txt > "${destination_folder}/${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt" # si potrebbe togliere il -u
          else
#            local last_report_in_output_dir_file_igmg=$(find "${destination_folder}" -type f -name "${REPORT_FILE_IGMG_NAME}_*.txt" | sort -n | tail -1)
#            sort -u ${last_report_in_output_dir_file_igmg} ${base_folder}/${REPORT_FILE_IGMG_NAME}*.txt \
#                    > "${destination_folder}/${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt"
            sort -u ${destination_folder}/${REPORT_FILE_IGMG_NAME}_*.txt ${base_folder}/${REPORT_FILE_IGMG_NAME}*.txt \
                    > "${destination_folder}/${REPORT_FILE_IGMG_NAME}_${CURTMS}.txt"
          fi
      fi

      local reports_in_input_dir_file_std=$(find "${base_folder}" -type f -name "${REPORT_FILE_STANDARD_NAME}*.txt" | wc -l)
      if [[ reports_in_input_dir_file_std -gt 0 ]]; then
          local reports_in_output_dir_file_std=$(find "${destination_folder}" -type f -name "${REPORT_FILE_STANDARD_NAME}_*.txt" | wc -l)
#          local reports_in_output_dir_file_std=$(ls -dq ${destination_folder}/${REPORT_FILE_STANDARD_NAME}_*.txt | wc -l)
          if [[ reports_in_output_dir_file_std -eq 0 ]]; then
#            cp -f \
#               "${base_folder}/${REPORT_FILE_STANDARD_NAME}.txt" \
#               "${destination_folder}/${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt"
            sort -u ${base_folder}/${REPORT_FILE_STANDARD_NAME}*.txt > "${destination_folder}/${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt" # si potrebbe togliere il -u
          else
#            local last_report_in_output_dir_file_std=$(find "${destination_folder}" -type f -name "${REPORT_FILE_STANDARD_NAME}_*.txt" | sort -n | tail -1)
#            sort -u ${last_report_in_output_dir_file_std} ${base_folder}/${REPORT_FILE_STANDARD_NAME}*.txt \
#                    > "${destination_folder}/${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt"
            sort -u ${destination_folder}/${REPORT_FILE_STANDARD_NAME}_*.txt ${base_folder}/${REPORT_FILE_STANDARD_NAME}*.txt \
                    > "${destination_folder}/${REPORT_FILE_STANDARD_NAME}_${CURTMS}.txt"
          fi
      fi
    fi

    if [[ "${REPORT_PDR}" == "true" ]]; then

      local igmg_num=$(find "${base_folder}" -type f -name "${REPORT_PDR_PREFIX}*IGMG*.txt" | wc -l)
#      local igmg_num=$(ls -dq "${base_folder}/${REPORT_PDR_PREFIX}*IGMG*.txt" | wc -l)
      if [[ igmg_num -gt 0 ]]; then
          mkdir -p "${tmp_folder}"
#          find "${base_folder}" \
#                  -type f \
#                  -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \
#                  -exec cat {} \; \
#                  > "${tmp_folder}/TMP_aggregazione_IGMG.txt"
#          alternativa probabilmente più efficiente:
          find "${base_folder}" \
                  -type f \
                  -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \
                  | xargs cat \
                  > "${tmp_folder}/TMP_aggregazione_IGMG.txt"
  
          local reports_in_output_dir_pdr_igmg=$(find "${destination_folder}" -type f -name "${REPORT_PDR_IGMG_NAME}_*.txt" | wc -l)
#          local reports_in_output_dir_pdr_igmg=$(ls -dq ${destination_folder}/${REPORT_PDR_IGMG_NAME}_*.txt | wc -l)
          if [[ reports_in_output_dir_pdr_igmg -eq 0 ]]; then
#              echo "${CSV_HEADER_PDR}" > "${destination_folder}/${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt" # riaggiungo header tolto da sed
              sort -u "$tmp_folder/TMP_aggregazione_IGMG.txt" > "${destination_folder}/${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt" # si potrebbe togliere il -u
          else
#              local last_report_in_output_dir_pdr_igmg=$(find "${destination_folder}" -type f -name "${REPORT_PDR_IGMG_NAME}_*.txt" | sort -n | tail -1)
#              sort -u ${last_report_in_output_dir_pdr_igmg} "${tmp_folder}/TMP_aggregazione_IGMG.txt" \
#                    > "${destination_folder}/${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt"
              sort -u ${destination_folder}/${REPORT_PDR_IGMG_NAME}_*.txt "${tmp_folder}/TMP_aggregazione_IGMG.txt" \
                    > "${destination_folder}/${REPORT_PDR_IGMG_NAME}_${CURTMS}.txt"
          fi
      fi

      local std_num=$(find "${base_folder}" -type f \( -name "${REPORT_PDR_PREFIX}*.txt" -a ! -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \) | wc -l)
      if [[ std_num -gt 0 ]]; then
          mkdir -p "${tmp_folder}"
#          find "${base_folder}" \
#                  -type f \
#                  \( -name "${REPORT_PDR_PREFIX}*.txt" -a ! -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \) \
#                  -exec cat {} \; \
#                  > "${tmp_folder}/TMP_Aggregazione_STD.txt"
#          alternativa probabilmente più efficiente:
          find "${base_folder}" \
                  -type f \
                  \( -name "${REPORT_PDR_PREFIX}*.txt" -a ! -name "${REPORT_PDR_PREFIX}*IGMG*.txt" \) \
                  | xargs cat \
                  > "${tmp_folder}/TMP_Aggregazione_STD.txt"
  
          local reports_in_output_dir_pdr_std=$(find "${destination_folder}" -type f -name "${REPORT_PDR_STANDARD_NAME}_*.txt" | wc -l)
#          local reports_in_output_dir_pdr_std=$(ls -dq ${destination_folder}/${REPORT_PDR_STANDARD_NAME}_*.txt | wc -l)
          if [[ reports_in_output_dir_pdr_std -eq 0 ]]; then
#              echo "${CSV_HEADER_PDR}" > "${destination_folder}/${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt"  # riaggiungo header tolto da sed
              sort -u "$tmp_folder/TMP_Aggregazione_STD.txt" > "${destination_folder}/${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt" # si potrebbe togliere il -u
          else
#              local last_report_in_output_dir_pdr_std=$(find "${destination_folder}" -type f -name "${REPORT_PDR_STANDARD_NAME}_*.txt" | sort -n | tail -1)
#              sort -u ${last_report_in_output_dir_pdr_std} "${tmp_folder}/TMP_Aggregazione_STD.txt" \
#                    > "${destination_folder}/${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt"
              sort -u ${destination_folder}/${REPORT_PDR_STANDARD_NAME}_*.txt "${tmp_folder}/TMP_Aggregazione_STD.txt" \
                    > "${destination_folder}/${REPORT_PDR_STANDARD_NAME}_${CURTMS}.txt"
          fi
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

mkdir -p $TMP_PATH

cd "${BASE_PATH}"
find . -mindepth 5 -maxdepth 5 -type d | while read sotteso; do

    printf "%s\0" "${sotteso:2}"

done | xargs -0 -n 1 -P ${merge.report.processes} bash -c 'merge_pdr_reports_day "$@"' --

rm -rf $TMP_PATH
