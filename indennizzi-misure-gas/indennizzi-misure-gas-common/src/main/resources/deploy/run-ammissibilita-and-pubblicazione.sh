#!/bin/sh

# This file should be put in ${isilon.deploy.path}/deploy
# Usage:
# nohup ${isilon.deploy.path}/deploy/run-ammissibilita-and-pubblicazione.sh >> ${isilon.deploy.path}/deploy/log/cig-ammissibilita-pubblicazione_`date +\%Y\%m\%d\%H\%M\%S`.log
# Args:
# -a <path/to/params.properties> for ammissibilita rendiconti RZG1 job
# -p <path/to/params.properties> for pubblicazione rendiconti RZG2 job
# -r for recovery mode (used by ammissibilita_rendiconti job, the recovery file path should be put in params.properties)

EXECUTIONID=$(($(date +%s%N)/1000000))

AMMISSIBILITA_RENDICONTI_DEPLOY_PATH=${ammissibilitaRendiconti.isilon.deploy.path}
PUBBLICAZIONE_RENDICONTI_DEPLOY_PATH=${pubblicazioneRendiconti.isilon.deploy.path}

AMMISSIBILITA_RENDICONTI_PROPERTIES_PATH=${ammissibilitaRendiconti.hdfs.deploy.path}/params.properties
PUBBLICAZIONE_RENDICONTI_PROPERTIES_PATH=${pubblicazioneRendiconti.hdfs.deploy.path}/params.properties

errorMessage=0

while getopts ":a:p:r" opt; do
  case $opt in
    a) AMMISSIBILITA_RENDICONTI_PROPERTIES_PATH="${OPTARG:-$AMMISSIBILITA_RENDICONTI_PROPERTIES_PATH}"
    ;;
    p) PUBBLICAZIONE_RENDICONTI_PROPERTIES_PATH="${OPTARG:-$PUBBLICAZIONE_RENDICONTI_PROPERTIES_PATH}"
    ;;
    r) RECOVERY_MODE="-r"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

if [[(${errorMessage} -eq 0)]];
then
  echo "Running ammissibilita RZG1..."
  echo "Process executionid: $EXECUTIONID"
  $AMMISSIBILITA_RENDICONTI_DEPLOY_PATH/run-ammissibilita-rendiconti.sh -p $AMMISSIBILITA_RENDICONTI_PROPERTIES_PATH $RECOVERY_MODE -e $EXECUTIONID
  errorMessage=$?
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Ammissibilita RZG1 exited with error."
else
  echo "Ammissibilita RZG1 succeeded, starting pubblicazione RZG2..."
  echo  "Executionid to read: $EXECUTIONID"
  $PUBBLICAZIONE_RENDICONTI_DEPLOY_PATH/run-pubblicazione-rendiconti.sh -p $PUBBLICAZIONE_RENDICONTI_PROPERTIES_PATH -e $EXECUTIONID
  errorMessage=$?
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Pubblicazione RZG2 exited with error."
else
  echo "Ammissibilita RZG1 and pubblicazione RZG2 succeded."
fi