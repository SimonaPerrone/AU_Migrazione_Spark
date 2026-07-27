DATA_OGGI=$(date "+%Y%m%d%H%M%S")
FLOW=SBG
LOGFILE=${deploy.path.logs}/${DATA_OGGI}-MID-${FLOW}-log.txt

DRIVER_CORE=3
DRIVER_MEMORY=2G
NUM_EXECUTOR=12
EXECUTOR_CORES=2
EXECUTOR_MEMORY=4G
EXECUTOR_MEMORYOVERHEAD=1G
SHUFFLE_PARTITION=200

spark2-submit \
--class it.eng.au.mid.Driver \
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
${deploy.path.local}/meccanismo-incentivante-distributori-gas.jar -p ${deploy.path.hdfs}/params_sbg.properties -f ${FLOW} 2>&1 | tee -a ${LOGFILE}
