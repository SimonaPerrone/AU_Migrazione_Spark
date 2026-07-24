#!/bin/bash
DEPLOY_PATH=${deploy.path}
DEPLOY_MODE=${ingestion.deploy.mode}
LOG_CURRENT_TIMESTAMP=$(date +'%Y-%m-%d_%H:%M:%S')
[[ "${DEPLOY_MODE}" == "client" ]] && CONF_FOLDER="${DEPLOY_PATH}/conf" || CONF_FOLDER="conf"

spark2-submit \
    --class it.au.misure.ingestionMisureGasUnico.driver.FlowDriver \
    --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --num-executors ${ingestion.spark.num.executors} \
    --executor-cores ${ingestion.spark.executor.cores} \
    --executor-memory ${ingestion.spark.executor.memory} \
    --driver-memory ${ingestion.spark.driver.memory} \
    --files file:${DEPLOY_PATH}/conf/log4j-ingestion.properties#conf/log4j-ingestion.properties,${DEPLOY_PATH}/conf/application.conf#conf/application.conf,${DEPLOY_PATH}/conf/ammissibilita.conf#conf/ammissibilita.conf \
    --driver-java-options -Dconfig.file=${CONF_FOLDER}/application.conf \
    --conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=${CONF_FOLDER}/application.conf -Dlog4j.configuration=file:${CONF_FOLDER}/log4j-ingestion.properties -DcurrentDate=$LOG_CURRENT_TIMESTAMP" \
    --conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=conf/application.conf -DcurrentDate=$LOG_CURRENT_TIMESTAMP" \
    --conf spark.executor.memoryOverhead=${spark.executor.memoryOverhead} \
    ${DEPLOY_PATH}/ingestion-misure-gas-unico.jar "$@"