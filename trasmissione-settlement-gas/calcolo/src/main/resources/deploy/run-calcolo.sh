#!/bin/sh

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties

[[ "${PWD}" == *"/yarn/nm"* ]] && ISILON_DEPLOY_PATH="${PWD}" || ISILON_DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

while getopts ":p:m:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}" ;;
    m) ANNOMESE="-m ${OPTARG}" ;;
    \?) echo "Invalid option -$OPTARG" >&2 ;;
  esac
done

spark2-submit \
--class it.eng.au.calcoloSettlementGas.Driver \
--master yarn \
--deploy-mode client \
--num-executors 30 \
--executor-cores 4 \
--executor-memory 30G \
--driver-cores 4 \
--driver-memory 30G \
--files $ISILON_DEPLOY_PATH/log4j.properties \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ISILON_DEPLOY_PATH/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$ISILON_DEPLOY_PATH/log4j.properties" \
--conf spark.executor.memoryOverhead=4096 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.initialExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=30 \
--conf spark.driver.maxResultSize=4g \
$ISILON_DEPLOY_PATH/calcolo-tsg.jar -p $PROPERTIES_PATH $ANNOMESE