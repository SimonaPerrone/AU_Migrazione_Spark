# lancia prima processo forniture e poi processo misure puntando alle properties con soglia
# caricamento delta a -1 per forzare caricamento FULL

# da schedulare al primo lunedì del mese

set -e

session="PC-forniture-misure-gas-full-forzato"
DATA_OGGI=$(date "+%Y%m%d%H%M%S")
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-FORNITURE_MISURE_GAS_FULL_FORZATO-log.txt

DRIVER_CORE=3
DRIVER_MEMORY=15G
NUM_EXECUTOR=60
EXECUTOR_CORES=3
EXECUTOR_MEMORY=40G
EXECUTOR_MEMORYOVERHEAD=8G
SHUFFLE_PARTITION=400

echo "Avvio processo forniture" | tee -a ${LOGFILE}

spark2-submit \
--class it.eng.au.portaleConsumi.Driver \
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
${deploy.path.local}/portale-consumi.jar -p ${deploy.path.hdfs}/params_full.properties -f FORNITURE_GAS 2>&1 | tee -a ${LOGFILE}

echo "Avvio processo misure" | tee -a ${LOGFILE}

spark2-submit \
--class it.eng.au.portaleConsumi.Driver \
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
${deploy.path.local}/portale-consumi.jar -p ${deploy.path.hdfs}/params_full.properties -f MISURE_GAS -i FULL 2>&1 | tee -a ${LOGFILE}

source ${deploy.path.local}/refresh_impala.sh ${hive.table.forniture_misure_gas} ${LOGFILE}

echo "Fine processo" | tee -a ${LOGFILE}
