#!/bin/bash
deploy_path=${deploy.path.erp}
DATE=$(date +'%Y-%m-%d_%H:%M:%S')

echo "Start calcolo forniture process..."

spark2-submit \
    --class it.eng.au.ERP.Main \
    --master yarn \
    --num-executors ${spark.num.executors} \
    --executor-cores ${spark.executor.cores} \
    --executor-memory ${spark.executor.memory} \
    --driver-cores ${spark.driver.cores} \
    --driver-memory ${spark.driver.memory} \
    --conf spark.executor.memoryOverhead=${spark.executor.memoryOverhead} \
    --conf spark.dynamicAllocation.enabled=${spark.dynamicAllocation.enabled} \
    --conf spark.dynamicAllocation.minExecutors=${spark.dynamicAllocation.minExecutors} \
    --conf spark.dynamicAllocation.maxExecutors=${spark.dynamicAllocation.maxExecutors} \
    --conf spark.dynamicAllocation.initialExecutors=${spark.dynamicAllocation.initialExecutors} \
    --files file:${deploy.path.erp}/conf/log4j.properties,file:${deploy.path.erp}/conf/application.conf \
    --conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration.file=${deploy.path.erp}/conf/log4j.properties -Dconfig.file=${deploy.path.erp}/conf/application.conf -DcurrentDate=$DATE" \
    --conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=application.conf -DcurrentDate=$DATE" \
    --driver-java-options -Dconfig.file=${deploy.path.erp}/conf/application.conf \
    ${deploy.path.erp}/ERP.jar "$@"