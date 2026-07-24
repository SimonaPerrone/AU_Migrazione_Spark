#!/bin/sh

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties
DEPLOY_PATH=${isilon.deploy.path}
MODES="CC,CSEA,ID,UDD,UDB,AMM"

while getopts ":p:m:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}" ;;
    m) MODES="${OPTARG:-$MODES}" ;;
    \?) echo "Invalid option -$OPTARG" >&2 ;;
  esac
done

spark2-submit \
--class it.eng.au.scambioDatiGasivori.Driver \
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
$DEPLOY_PATH/pubblicazione-dati-gasivori.jar -p $PROPERTIES_PATH -m $MODES