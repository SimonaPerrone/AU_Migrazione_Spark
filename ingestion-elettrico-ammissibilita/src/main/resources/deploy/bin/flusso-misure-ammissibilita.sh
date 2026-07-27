#!/bin/bash

DRIVER_CORE=6
DRIVER_MEMORY=30G
NUM_EXECUTOR=65
EXECUTOR_CORES=6
EXECUTOR_MEMORY=32G
DRIVER_MAXRESULTSIZE=2G
DEPLOY_PATH=${deploy.path}
DEPLOY_PATH_OLD=${deploy.path.old}
DEPLOY_MODE=${deploy.mode}
FILES=file:${DEPLOY_PATH}/conf/log4j.properties#conf/log4j.properties,${DEPLOY_PATH}/conf/application.conf#conf/application.conf,${DEPLOY_PATH}/conf/ammissibilita.conf#conf/ammissibilita.conf

LOG_CURRENT_TIMESTAMP=$(date +'%Y-%m-%d_%H:%M:%S')
[[ "${DEPLOY_MODE}" == "client" ]] && CONF_FOLDER="${DEPLOY_PATH}/conf" || CONF_FOLDER="conf"

while getopts ":d:r:n:c:m:gGD:M:Y:S" OPT; do
  case "$OPT" in
    d) DRIVER_CORE="${OPTARG:-$DRIVER_CORE}" ;;
    r) DRIVER_MEMORY="${OPTARG:-$DRIVER_MEMORY}" ;;
    n) NUM_EXECUTOR="${OPTARG:-$NUM_EXECUTOR}" ;;
    c) EXECUTOR_CORES="${OPTARG:-$EXECUTOR_CORES}" ;;
    m) EXECUTOR_MEMORY="${OPTARG:-$EXECUTOR_MEMORY}" ;;
    g) FLUSSI_1G="-g" ;;
    G) FLUSSI_2G="-G" ;;
    D) GIORNO="--giorno ${OPTARG}" ;;
    M) MESE="--mese ${OPTARG}" ;;
    Y) ANNO="--anno ${OPTARG}" ;;
    S) SMIS="-S" ;;
    \?) echo "Invalid option -$OPTARG" >&2 ; exit 2 ;;
  esac
done

spark2-submit \
    --class it.eng.au.ammissibilita.CheckAmmissibilitaDriver \
    --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --num-executors ${NUM_EXECUTOR} \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --driver-memory ${DRIVER_MEMORY} \
    --files $FILES \
    --driver-java-options -Dconfig.file=${CONF_FOLDER}/application.conf \
    --conf spark.driver.maxResultSize=${DRIVER_MAXRESULTSIZE} \
    --conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -DPROP_FILE=${DEPLOY_PATH_OLD}/job.properties -Dconfig.file=${CONF_FOLDER}/application.conf -Dlog4j.configuration=file:${CONF_FOLDER}/log4j.properties -DcurrentDate=$LOG_CURRENT_TIMESTAMP" \
    --conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=conf/application.conf -DcurrentDate=$LOG_CURRENT_TIMESTAMP" \
    ${DEPLOY_PATH}/bin/flusso-misure-ammissibilita.jar $FLUSSI_1G $FLUSSI_2G $SMIS $ANNO $MESE $GIORNO