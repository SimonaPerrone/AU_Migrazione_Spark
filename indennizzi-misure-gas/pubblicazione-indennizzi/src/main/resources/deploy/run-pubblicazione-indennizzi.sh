#!/bin/sh

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties

while getopts ":p:e:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}"
    ;;
    e) INPUT_TABLE_EXECUTIONID="-e ${OPTARG}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

spark2-submit \
--class it.eng.au.pubblicazioneIndennizzi.Driver \
--master yarn \
--deploy-mode client \
--num-executors 5 \
--executor-cores 3 \
--executor-memory 21G \
--driver-cores 3 \
--driver-memory 21G \
--files ${isilon.deploy.path}/log4j.properties \
--conf "spark.dynamicAllocation.enabled=false" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf spark.executor.memoryOverhead=4096 \
${isilon.deploy.path}/pubblicazione-indennizzi.jar -p $PROPERTIES_PATH $INPUT_TABLE_EXECUTIONID

echo "run remove file in tmp folder"
rm -r ${isilon.basepath.tmp}/tmp/CIG/CIG1/*
rm -r ${isilon.basepath.tmp}/tmp/CIG/CIG2/*
echo "end remove file in tmp folder"