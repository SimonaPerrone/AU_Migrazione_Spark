session="PC-misure-gas-3m"
DATA_OGGI=$(date "+%Y%m%d%H%M%S")
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-MISURE_GAS_3M-log.txt

DRIVER_CORE=3
DRIVER_MEMORY=15G
NUM_EXECUTOR=60
EXECUTOR_CORES=3
EXECUTOR_MEMORY=40G
EXECUTOR_MEMORYOVERHEAD=8G
SHUFFLE_PARTITION=400

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
${deploy.path.local}/portale-consumi.jar -p ${deploy.path.hdfs}/params.properties -f MISURE_GAS -i 3M 2>&1 | tee -a ${LOGFILE}

source ${deploy.path.local}/refresh_impala.sh ${hive.table.forniture_misure_gas} ${LOGFILE}
