#!/bin/bash
# lancio processo Spark pubblicazioni da parametrizzare con:
DATA_RICHIESTA="$1"
FLOW="$2"

DATA_OGGI=$(date "+%Y%m%d%H%M%S")
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-CCE-PUBBLICAZIONE_${DATA_RICHIESTA}_${FLOW}.log

DRIVER_CORE=3
DRIVER_MEMORY=15G
NUM_EXECUTOR=60
EXECUTOR_CORES=3
EXECUTOR_MEMORY=40G
EXECUTOR_MEMORYOVERHEAD=12G
SHUFFLE_PARTITION=600

spark2-submit \
--class it.eng.au.pubblicazione_cce.Driver \
--master yarn \
--deploy-mode client \
--executor-cores $EXECUTOR_CORES \
--executor-memory $EXECUTOR_MEMORY \
--driver-memory $DRIVER_MEMORY \
--driver-cores $DRIVER_CORE \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=$NUM_EXECUTOR \
--conf spark.dynamicAllocation.initialExecutors=2 \
--files ${deploy.path.local}/log4j.properties \
--conf "spark.driver.extraJavaOptions=-Xss512m -XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path.local}/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Xss512m -XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path.local}/log4j.properties" \
--conf spark.sql.shuffle.partitions=$SHUFFLE_PARTITION \
--conf spark.executor.memoryOverhead=$EXECUTOR_MEMORYOVERHEAD \
${deploy.path.local}/pubblicazione_cce.jar -p ${deploy.path.hdfs}/params.properties -d ${DATA_RICHIESTA} -f ${FLOW} | tee -a ${LOGFILE}
