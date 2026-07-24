#!/usr/bin/env bash
set -e

DEPLOY_PATH=${deploy.path}
HIVE_DYNAMIC_PROPERTIES="SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict;"
REPORTS_LIST_CSV_FOLDER=${merge.report.tracking.folder}
export REPORTS_FILE_LIST_CSV_FOLDER="${REPORTS_LIST_CSV_FOLDER}/file"
export REPORTS_PDR_LIST_CSV_FOLDER="${REPORTS_LIST_CSV_FOLDER}/pdr"

echo "Merge e pubblicazione dei reports di ammissibilità in corso..."
"$DEPLOY_PATH"/report/merge_track_reports_v3.1.sh "$@"

echo "Tracking reports base file in corso..."
if [[ -d "${REPORTS_FILE_LIST_CSV_FOLDER}" && -n "$(ls -A ${REPORTS_FILE_LIST_CSV_FOLDER})" ]]; then
  hive -e "LOAD DATA LOCAL INPATH '${REPORTS_FILE_LIST_CSV_FOLDER}' OVERWRITE INTO TABLE ${hive.cmg_gas}.report_ammissibilita_file_gas_tracking_staging;"
  hive -e "${HIVE_DYNAMIC_PROPERTIES} INSERT INTO ${hive.cmg_gas}.report_ammissibilita_file_gas_tracking PARTITION(annomese) SELECT cartella_cloud, report_filename, input_filename, d_creazione_report, annomesegiornodir, annomese FROM ${hive.cmg_gas}.report_ammissibilita_file_gas_tracking_staging;"
fi

echo "Tracking reports base pdr in corso..."
if [[ -d "${REPORTS_PDR_LIST_CSV_FOLDER}" && -n "$(ls -A ${REPORTS_PDR_LIST_CSV_FOLDER})" ]]; then
  hive -e "LOAD DATA LOCAL INPATH '${REPORTS_PDR_LIST_CSV_FOLDER}' OVERWRITE INTO TABLE ${hive.cmg_gas}.report_ammissibilita_pdr_tracking_staging;"
  hive -e "${HIVE_DYNAMIC_PROPERTIES} INSERT INTO ${hive.cmg_gas}.report_ammissibilita_pdr_tracking PARTITION(annomese) SELECT cartella_cloud, report_filename, input_filename, cod_pdr, d_creazione_report, annomesegiornodir, annomese FROM ${hive.cmg_gas}.report_ammissibilita_pdr_tracking_staging;"
fi

rm -rf ${REPORTS_FILE_LIST_CSV_FOLDER}
rm -rf ${REPORTS_PDR_LIST_CSV_FOLDER}

echo "Procedura completata."




