#!/bin/sh

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties

while getopts ":p:r:m:d:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}"
    ;;
    r) RECOVERY_MODE="-r"
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

spark2-submit \
--class it.eng.au.calcoloIndennizzi.Driver \
--master yarn \
--deploy-mode client \
--num-executors 30 \
--executor-cores 3 \
--executor-memory 21G \
--driver-cores 3 \
--driver-memory 21G \
--files ${isilon.deploy.path}/log4j.properties \
--conf "spark.dynamicAllocation.enabled=false" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf spark.executor.memoryOverhead=4096 \
${isilon.deploy.path}/calcolo-indennizzi.jar -p $PROPERTIES_PATH $RECOVERY_MODE $YEAR_MONTH $TGL_THRESHOLD_DAY