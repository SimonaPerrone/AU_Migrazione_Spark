#export SPARK_SUBMIT=$(cat <<-EOF
DATA_CALC=$1
X=$2
Y=$3
PRINT_VERBOSE=$4

DEPLOY_MODE=${deploy.mode}
# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"
[[ "${DEPLOY_MODE}" == "client" ]] && CONF_FOLDER="${DEPLOY_PATH}/conf" || CONF_FOLDER="conf"


spark-submit \
    --class it.au.misure.calcolo_capacita.Main \
    --master yarn \
    --deploy-mode ${DEPLOY_MODE} \
    --num-executors ${spark.num.executors} \
    --executor-cores ${spark.executor.cores} \
    --executor-memory ${spark.executor.memory} \
    --driver-memory ${spark.driver.memory} \
    --files file:${DEPLOY_PATH}/conf/log4j.properties#conf/log4j.properties,${DEPLOY_PATH}/conf/application.conf#conf/application.conf \
    --driver-java-options -Dconfig.file=${CONF_FOLDER}/application.conf \
    --conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=${CONF_FOLDER}/application.conf -Dlog4j.configuration=file:${CONF_FOLDER}/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=conf/application.conf" \
    --conf spark.executor.memoryOverhead=${spark.memoryoverhead} \
    --conf spark.default.parallelism=${spark.parallelism} \
    --conf spark.sql.shuffle.partitions=${spark.shuffle} \
    ${DEPLOY_PATH}/calcolo-capacita.jar "$DATA_CALC" "$X" "$Y" "$PRINT_VERBOSE"
#EOF
#)