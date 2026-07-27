#!/bin/bash

DRIVER_CORE=3
DRIVER_MEMORY=15G
NUM_EXECUTOR=40
EXECUTOR_CORES=3
EXECUTOR_MEMORY=30G
EXECUTOR_MEMORYOVERHEAD=10G
SHUFFLE_PARTITION=2000
PROPERTIES=${hdfs.path}/params.properties

while getopts ":d:r:n:c:m:x:h:p:" opt; do
  case $opt in
    d) DRIVER_CORE="${OPTARG:-$DRIVER_CORE}"
    ;;
    r) DRIVER_MEMORY="${OPTARG:-$DRIVER_MEMORY}"
    ;;
    n) NUM_EXECUTOR="${OPTARG:-$NUM_EXECUTOR}"
    ;;
    c) EXECUTOR_CORES="${OPTARG:-$EXECUTOR_CORES}"
    ;;
    m) EXECUTOR_MEMORY="${OPTARG:-$EXECUTOR_MEMORY}"
    ;;
    x) EXECUTOR_MEMORYOVERHEAD="${OPTARG:-$EXECUTOR_MEMORYOVERHEAD}"
    ;;
    h) SHUFFLE_PARTITION="${OPTARG:-$SHUFFLE_PARTITION}"
    ;;
    p) PROPERTIES="${OPTARG:-$PROPERTIES}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

spark2-submit \
--class it.eng.au.aggregatoreConsumiAgg.Driver \
--master yarn \
--deploy-mode client \
--num-executors $NUM_EXECUTOR \
--executor-cores $DRIVER_CORE \
--executor-memory $EXECUTOR_MEMORY \
--driver-memory $DRIVER_MEMORY \
--driver-cores $DRIVER_CORE \
--files ${deploy.path}/log4j.properties \
--conf spark.executor.memoryOverhead=$EXECUTOR_MEMORYOVERHEAD \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${deploy.path}/log4j.properties" \
--conf spark.sql.shuffle.partitions=$SHUFFLE_PARTITION \
--conf spark.network.timeout=600 \
--conf spark.executor.heartbeatInterval=100 \
${deploy.path}/aggregatore-agg.jar $PROPERTIES


echo "Emptying temporary folder.."
rm -r ${isilon.basepath.tmp}/tmp/AGG/AGG1/*
rm -r ${isilon.basepath.tmp}/tmp/AGG/AGG2/*
rm -r ${isilon.basepath.tmp}/tmp/AGG/AGG3/*
rm -r ${isilon.basepath.tmp}/tmp/AGG/AGG4/*
rm -r ${isilon.basepath.tmp}/tmp/AGG/AGG5/*
echo "Temporary folder emptied."