#!/bin/sh

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties
[[ "${PWD}" == *"/yarn/nm"* ]] && dEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

while getopts ":p:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}" ;;
    \?) echo "Invalid option -$OPTARG" >&2 ;;
  esac
done

spark2-submit \
--class it.eng.au.gse.calcoloMensile.Driver \
--master yarn \
--deploy-mode client \
--executor-cores 5 \
--executor-memory 35G \
--driver-memory 35G \
--driver-cores 5 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=12 \
--conf spark.dynamicAllocation.initialExecutors=4 \
--files $DEPLOY_PATH/log4j-cm.properties \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$DEPLOY_PATH/log4j-cm.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$DEPLOY_PATH/log4j-cm.properties" \
--conf spark.executor.memoryOverhead=4096 \
$DEPLOY_PATH/gse-calcolo-mensile.jar -p $PROPERTIES_PATH