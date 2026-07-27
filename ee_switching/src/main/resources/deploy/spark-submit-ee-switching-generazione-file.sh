#!/usr/bin/env bash
set -e

# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

DEPLOY_MODE=${deploy.mode}
[[ "${DEPLOY_MODE}" == "client" ]] && CONF_FOLDER="${DEPLOY_PATH}/conf" || CONF_FOLDER="conf"
DATE=$(date +'%Y-%m-%d_%H:%M:%S')

if [[ "$*" == *"funzionali"* ]]; then
    LOG4J_LOG_NAME="funzionali_log4j.properties"
elif [[ "$*" == *"storici"* ]]; then
    LOG4J_LOG_NAME="storici_log4j.properties"
else
    echo "-n|--flow-name is required"
    exit 2
fi

die()
{
    echo "$*" >&2
    exit 2
}

needs_arg()
{
    if [ -z "$OPTARG" ];
    then
        die "No arg for --$OPT option"
    fi
}

while getopts n:t:p:f:F:s:d:D:c:q:-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    n | flow-name ) needs_arg ;;
    t | timestamp ) needs_arg ;;
    p | pod ) needs_arg ;;
    f | date-funzionali-switching ) needs_arg ;;
    F | date-funzionali-na ) needs_arg ;;
    s | date-storici-switching ) needs_arg ;;
    d | piva-distributore ) needs_arg; ;;
    D | piva-udd ) needs_arg ;;
    c | coppie-piva ) needs_arg ;;
    q | queue ) needs_arg ; QUEUE_OPTION="--queue ${OPTARG}" ;;
    ??* ) die "Illegal option --$OPT" ;;
    \? ) exit 2 ;;
  esac
done

spark2-submit \
    --class it.au.misure.ee_switching.driver.FlowDriver \
    --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --num-executors ${spark.num.executors} \
    --executor-cores ${spark.executor.cores} \
    --executor-memory ${spark.executor.memory} \
    --driver-memory ${spark.driver.memory} \
    ${QUEUE_OPTION} \
    --files "file:${DEPLOY_PATH}/conf/${LOG4J_LOG_NAME}""#conf/${LOG4J_LOG_NAME}",${DEPLOY_PATH}/conf/application.conf#conf/application.conf \
    --driver-java-options -Dconfig.file=${CONF_FOLDER}/application.conf \
    --conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=${CONF_FOLDER}/application.conf -Dlog4j.configuration=file:${CONF_FOLDER}/${LOG4J_LOG_NAME}" \
    --conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=conf/application.conf" \
    ${DEPLOY_PATH}/ee-switching.jar "$@"