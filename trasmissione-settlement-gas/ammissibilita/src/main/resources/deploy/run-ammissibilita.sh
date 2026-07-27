#!/bin/sh

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties

[[ "${PWD}" == *"/yarn/nm"* ]] && ISILON_DEPLOY_PATH="${PWD}" || ISILON_DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

while getopts ":p:m:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}" ;;
    m) ANNOMESE="-m ${OPTARG}" ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

spark2-submit \
--class it.eng.au.ammissibilitaSettlementGas.Driver \
--master yarn \
--deploy-mode client \
--num-executors 30 \
--executor-cores 3 \
--executor-memory 21G \
--driver-cores 3 \
--driver-memory 21G \
--files $ISILON_DEPLOY_PATH/log4j.properties \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ISILON_DEPLOY_PATH/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$ISILON_DEPLOY_PATH/log4j.properties" \
--conf spark.executor.memoryOverhead=4096 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.initialExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=30 \
$ISILON_DEPLOY_PATH/ammissibilita-tsg.jar -p $PROPERTIES_PATH $ANNOMESE