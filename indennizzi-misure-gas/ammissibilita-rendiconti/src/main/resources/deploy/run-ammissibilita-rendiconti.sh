#!/bin/sh

DATE=$(date +'%Y-%m-%d')
DEPLOY_PATH=${isilon.deploy.path}
MAILLOG_PATH=${isilon.mailLog.path}
LOGFILE_NAME=$MAILLOG_PATH/ammissibilita-rendiconti-mail-$DATE.log

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
$DEPLOY_PATH/ammissibilita-rendiconti.jar -p $PROPERTIES_PATH $RECOVERY_MODE $EXECUTIONID
errorMessage=$?

# if everything is okay, we rename the log file appending "success"
if [[ (${errorMessage} -eq 0) ]];
then
  echo "Process successfully terminated."
  mv $LOGFILE_NAME ${LOGFILE_NAME/.log/-success.log}
# otherwise we rename it appending "error"
else
  echo "Process terminated with error."
  mv $LOGFILE_NAME ${LOGFILE_NAME/.log/-error.log}
fi