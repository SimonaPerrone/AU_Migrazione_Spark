#!/bin/sh

# This file has been left here to save modify_nifi_process_status function and all the stuff regarding nifi, for future use

DATE=$(date +'%Y-%m-%d')
DEPLOY_PATH=${isilon.deploy.path}
MAILLOG_PATH=${isilon.mailLog.path}
LOGFILE_NAME=MAILLOG_PATH/ammissibilita-rendiconti-mail-$DATE.log

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties

errorMessage=0

while getopts ":p:re:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}"
    ;;
    r) RECOVERY_MODE="-r"
    ;;
    e) EXECUTIONID="-e ${OPTARG}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

spark2-submit \
--class it.eng.au.ammissibilitaRendiconti.Driver \
--master yarn \
--deploy-mode client \
--num-executors 30 \
--executor-cores 3 \
--executor-memory 21G \
--driver-cores 3 \
--driver-memory 21G \
--files $DEPLOY_PATH/log4j.properties \
--conf "spark.dynamicAllocation.enabled=false" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$DEPLOY_PATH/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$DEPLOY_PATH/log4j.properties" \
--conf spark.executor.memoryOverhead=4096 \
$DEPLOY_PATH/ammissibilita-rendiconti.jar -p $PROPERTIES_PATH $RECOVERY_MODE
errorMessage=$?

# Function to modify a NiFi process status
modify_nifi_process_status(){
  client_id="$1"
  revision_version="$2"
  process_id="$3"
  nifi_host="$4"
  nifi_port="$5"
  state="$6"

  curl -XPUT -H 'Content-Type: application/json' -d '{
      "revision": {
        "clientId": "'${client_id}'",
        "version": '${revision_version}'
      },
      "component": {
        "id": "'${process_id}'",
        "state": "'${state}'"
      }
    }' http://"${nifi_host}":"${nifi_port}"/nifi-api/processors/"${process_id}"
}

# if everything is okay, we rename the log file renaming it from "error" to "success"
if [[ (${errorMessage} -eq 0) ]];
then
  echo "Process successfully terminated."
  mv $LOGFILE_NAME ${LOGFILE_NAME/.log/-success.log}
# otherwise we start the nifi process to send a mail with the error log as attachment
else
  echo "Process terminated with error, sending e-mail using NiFi.."
  mv $LOGFILE_NAME ${LOGFILE_NAME/.log/-error.log}

  nifi_host=dmphclo06.siiau.local
  nifi_port=8080
  process_id=edadd77d-0185-1000-ffff-ffffc805ef5c

  client_id=$(curl -XGET http://${nifi_host}:${nifi_port}/nifi-api/processors/"${process_id}" | python -c "import sys, json; print(json.load(sys.stdin)['revision']['clientId'])")
  revision_version=$(curl -XGET http://${nifi_host}:${nifi_port}/nifi-api/processors/"${process_id}" | python -c "import sys, json; print(json.load(sys.stdin)['revision']['version'])")

  modify_nifi_process_status "${client_id}" "${revision_version}" "${process_id}" "${nifi_host}" "${nifi_port}" "RUNNING"
  sleep 60
  modify_nifi_process_status "${client_id}" "${revision_version}" "${process_id}" "${nifi_host}" "${nifi_port}" "STOPPED"
  echo "E-mail sent. Procedure terminated."
fi