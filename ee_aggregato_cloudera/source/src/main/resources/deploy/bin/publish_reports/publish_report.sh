#!/bin/bash
set -e

usage()
{
  echo "Usage: publish_reports.sh -f <1G/2G> -d <date (YYYY-MM-DD)>"
}

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

copy_and_track_reports()
{
  local relative_folder=$1
  local base_folder="$SYNC_TMP_ROOT_PATH/$relative_folder"
  local destination_folder="$SYNC_DEST_ROOT_PATH/$relative_folder"
  local now=$(date '+%Y-%m-%d %T.%6N')

  if ! [[ -d "${base_folder}" ]]; then
        return
  fi

  mkdir -p "${destination_folder}"
  mv -f \
       "${base_folder}/${FILE_REPORT_NAME}.txt" \
       "${destination_folder}/${FILE_REPORT_NAME}_${TIMESTAMP}.txt"
  #if the report has been correctly moved then we add its tracking info to a csv file
  report_copied=$?
  if [[ $report_copied -eq 0 ]]
  then
    awk -v report="${FILE_REPORT_NAME}_${TIMESTAMP}.txt" -v ts="$now" -v amgdir="${ANNO}${MESE}${GIORNO}" -v am="${ANNO}${MESE}" \
        --field-separator "\"*;\"*" 'NR > 1 {print $1","report","$2","ts","amgdir","am}' \
        "${destination_folder}/${FILE_REPORT_NAME}_${TIMESTAMP}.txt" \
        >> "${REPORTS_FILE_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
  fi

  mv -f \
       "${base_folder}/${FILE_REPORT_NAME_SMIS}.txt" \
       "${destination_folder}/${FILE_REPORT_NAME_SMIS}_${TIMESTAMP}.txt"
  #if the report has been correctly moved then we add its tracking info to a csv file
  report_copied=$?
  if [[ $report_copied -eq 0 ]]
  then
    awk -v report="${FILE_REPORT_NAME_SMIS}_${TIMESTAMP}.txt" -v ts="$now" -v amgdir="${ANNO}${MESE}${GIORNO}" -v am="${ANNO}${MESE}" \
        --field-separator "\"*;\"*" 'NR > 1 {print $1","report","$2","ts","amgdir","am}' \
        "${destination_folder}/${FILE_REPORT_NAME_SMIS}_${TIMESTAMP}.txt" \
        >> "${REPORTS_FILE_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
  fi

  mv -f \
       "${base_folder}/${POD_REPORT_NAME}.txt" \
       "${destination_folder}/${POD_REPORT_NAME}_${TIMESTAMP}.txt"
  #if the report has been correctly moved then we add its tracking info to a csv file
  report_copied=$?
  if [[ $report_copied -eq 0 ]]
  then
    awk -v report="${POD_REPORT_NAME}_${TIMESTAMP}.txt" -v ts="$now" -v amgdir="${ANNO}${MESE}${GIORNO}" -v am="${ANNO}${MESE}" \
        --field-separator "\"*;\"*" 'NR > 1 {print $1","report","$2","$3","ts","amgdir","am}' \
        "${destination_folder}/${POD_REPORT_NAME}_${TIMESTAMP}.txt" \
        >> "${REPORTS_POD_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
  fi

  mv -f \
       "${base_folder}/${POD_REPORT_NAME_SMIS}.txt" \
       "${destination_folder}/${POD_REPORT_NAME_SMIS}_${TIMESTAMP}.txt"
  #if the report has been correctly moved then we add its tracking info to a csv file
  report_copied=$?
  if [[ $report_copied -eq 0 ]]
  then
    awk -v report="${POD_REPORT_NAME_SMIS}_${TIMESTAMP}.txt" -v ts="$now" -v amgdir="${ANNO}${MESE}${GIORNO}" -v am="${ANNO}${MESE}" \
        --field-separator "\"*;\"*" 'NR > 1 {print $1","report","$2","$3","ts","amgdir","am}' \
        "${destination_folder}/${POD_REPORT_NAME_SMIS}_${TIMESTAMP}.txt" \
        >> "${REPORTS_POD_LIST_CSV_FOLDER}/${relative_folder//\//_}.csv"
  fi

}
export -f copy_and_track_reports

# Handle options with getopts
while getopts f:d: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    f | flusso) needs_arg;
      case "$OPTARG" in
      "1G") SYNC_FOLDER=${sync.1g};SYNC_DEST_FOLDER=${sync.dest.1g} ;;
      "2G") SYNC_FOLDER=${sync.2g};SYNC_DEST_FOLDER=${sync.dest.2g};;
      *) echo "value $OPTARG not valid.";usage;die;;
      esac
    ;;
    d | date ) needs_arg; DATE="$OPTARG" ;;
    ??* )          usage; die "Illegal option --$OPT" ;;
    \? )           exit 2 ;;
  esac
done
shift $((OPTIND-1))

# Check mandatory fields
if ! [ -v SYNC_FOLDER ]
then
    usage
    die
fi
if ! [ -v DATE ]
then
    usage
    die
fi

#DEFINE VARS
export SYNC_TMP_ROOT_PATH="${sync.root}$SYNC_FOLDER"
export SYNC_DEST_ROOT_PATH="${sync.dest.root}$SYNC_DEST_FOLDER"
export REPORTS_FILE_LIST_CSV_FOLDER="${merge.report.tracking.folder}/file"
export REPORTS_POD_LIST_CSV_FOLDER="${merge.report.tracking.folder}/pod"
export FILE_REPORT_NAME="ReportAmmissibilitàFileEE"
export FILE_REPORT_NAME_SMIS="ReportAmmissibilitàFileSMIS"
export POD_REPORT_NAME="ReportEsitoPOD"
export POD_REPORT_NAME_SMIS="ReportEsitoPODSMIS"
export TIMESTAMP=$( date +%Y%m%d%H%M%S)
export ANNO=$(date -d "$DATE" +%Y)
export MESE=$(date -d "$DATE" +%m)
export GIORNO=$(date -d "$DATE" +%d)
export LOG_DIR="${deploy.path}/log/publish_reports"
export LOG_NAME="publish_reports_$(date +%Y-%m-%d-%H-%M-%S).log"
echo "Sync tmp root path $SYNC_TMP_ROOT_PATH"
echo "Sync dest root path $SYNC_DEST_ROOT_PATH"
echo "Logs at: $LOG_DIR/$LOG_NAME"

mkdir -p $LOG_DIR
mkdir -p -m 777 ${REPORTS_FILE_LIST_CSV_FOLDER}
mkdir -p -m 777 ${REPORTS_POD_LIST_CSV_FOLDER}

SECONDS=0

cd $SYNC_TMP_ROOT_PATH
find */*/*/$ANNO/$MESE$GIORNO -type d | xargs -n 1  bash -c 'copy_and_track_reports "$@"' --  &>> "$LOG_DIR/$LOG_NAME"

echo "[$(date +%Y-%m-%d-%H-%M-%S)] Ended moving reports in $SECONDS seconds" &>> "$LOG_DIR/$LOG_NAME"

#EXPORT TRACKING ON HIVE
SECONDS=0
HIVE_DYNAMIC_PROPERTIES="SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict;"
REPORTS_LIST_CSV_FOLDER=${merge.report.tracking.folder}
if [[ -d "${REPORTS_FILE_LIST_CSV_FOLDER}" && -n "$(ls -A ${REPORTS_FILE_LIST_CSV_FOLDER})" ]]; then
  hive -e "LOAD DATA LOCAL INPATH '${REPORTS_FILE_LIST_CSV_FOLDER}' OVERWRITE INTO TABLE ${hive.au}.${hive.table.report_ammissibilita_file_ee_tracking_staging};" &>> "$LOG_DIR/$LOG_NAME"
  hive -e "${HIVE_DYNAMIC_PROPERTIES} INSERT INTO ${hive.au}.${hive.table.report_ammissibilita_file_ee_tracking} PARTITION(annomese) SELECT cartella_cloud, report_filename, input_filename, d_creazione_report, annomesegiornodir, annomese FROM ${hive.au}.${hive.table.report_ammissibilita_file_ee_tracking_staging};" &>> "$LOG_DIR/$LOG_NAME"
  rm -rf ${REPORTS_FILE_LIST_CSV_FOLDER} &>> "$LOG_DIR/$LOG_NAME"
fi

if [[ -d "${REPORTS_POD_LIST_CSV_FOLDER}" && -n "$(ls -A ${REPORTS_POD_LIST_CSV_FOLDER})" ]]; then
  hive -e "LOAD DATA LOCAL INPATH '${REPORTS_POD_LIST_CSV_FOLDER}' OVERWRITE INTO TABLE ${hive.au}.${hive.table.report_ammissibilita_pod_tracking_staging};" &>> "$LOG_DIR/$LOG_NAME"
  hive -e "${HIVE_DYNAMIC_PROPERTIES} INSERT INTO ${hive.au}.${hive.table.report_ammissibilita_pod_tracking} PARTITION(annomese) SELECT cartella_cloud, report_filename, input_filename, cod_pod, d_creazione_report, annomesegiornodir, annomese FROM ${hive.au}.${hive.table.report_ammissibilita_pod_tracking_staging};" &>> "$LOG_DIR/$LOG_NAME"
  rm -rf ${REPORTS_PDR_LIST_CSV_FOLDER} &>> "$LOG_DIR/$LOG_NAME"
fi

echo "[$(date +%Y-%m-%d-%H-%M-%S)] Ended reports tracking in $SECONDS seconds" &>> "$LOG_DIR/$LOG_NAME"
