#!/bin/sh

# This file should be put in ${isilon.deploy.path}/deploy
# Usage:
# nohup ${isilon.deploy.path}/deploy/run-calcolo-and-pubblicazione.sh >> ${isilon.deploy.path}/deploy/log/cig-calcolo-pubblicazione_`date +\%Y\%m\%d\%H\%M\%S`.log
# Args:
# -c <path/to/params.properties> (optional) properties path for calcolo indennizzi IZG1 job
# -p <path/to/params.properties> (optional) properties path for pubblicazione indennizzi IZG1 job
# -m <yyyyMM> (optional) year-month to compute for calcolo indennizzi IZG1 job
# -d <d> (optional) tgl threshold day for calcolo indennizzi IZG1 job

DATE=$(date +'%Y-%m-%d_%H:%M:%S')

CALCOLO_INDENNIZZI_DEPLOY_PATH=${calcoloIndennizzi.isilon.deploy.path}
PUBBLICAZIONE_INDENNIZZI_DEPLOY_PATH=${pubblicazioneIndennizzi.isilon.deploy.path}

CALCOLO_INDENNIZZI_PROPERTIES_PATH=${calcoloIndennizzi.hdfs.deploy.path}/params.properties
PUBBLICAZIONE_INDENNIZZI_PROPERTIES_PATH=${pubblicazioneIndennizzi.hdfs.deploy.path}/params.properties

errorMessage=0

while getopts ":c:p:rm:d:" opt; do
  case $opt in
    c) CALCOLO_INDENNIZZI_PROPERTIES_PATH="${OPTARG:-$CALCOLO_INDENNIZZI_PROPERTIES_PATH}"
    ;;
    p) PUBBLICAZIONE_INDENNIZZI_PROPERTIES_PATH="${OPTARG:-$PUBBLICAZIONE_INDENNIZZI_PROPERTIES_PATH}"
    ;;
    r) CALCOLO_RECOVERY_MODE="-r"
    ;;
    m) YEAR_MONTH="-m ${OPTARG}"
    ;;
    d) TGL_THRESHOLD_DAY="-d ${OPTARG}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

if [[(${errorMessage} -eq 0)]];
then
  echo "Running calcolo indennizzi..."
  $CALCOLO_INDENNIZZI_DEPLOY_PATH/run-calcolo-indennizzi.sh -p $CALCOLO_INDENNIZZI_PROPERTIES_PATH $RECOVERY_MODE $YEAR_MONTH $TGL_THRESHOLD_DAY
  errorMessage=$?
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Calcolo IZG1 exited with error."
else
  echo "Calcolo IZG1 succeded, starting pubblicazione IZG1..."
  $PUBBLICAZIONE_INDENNIZZI_DEPLOY_PATH/run-pubblicazione-indennizzi.sh -p $PUBBLICAZIONE_INDENNIZZI_PROPERTIES_PATH
  errorMessage=$?

  if [[(${errorMessage} -ne 0)]];
  then
    echo "Pubblicazione IZG1 exited with error."
  fi
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Calcolo e pubblicazione IZG1 exited with error."
else
  echo "Calcolo e pubblicazione IZG1 succeded."
fi