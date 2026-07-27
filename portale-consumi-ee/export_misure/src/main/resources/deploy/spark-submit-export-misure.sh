#!/bin/bash
deploy_path=${deploy.path.export_misure}
DATE=$(date +'%Y-%m-%d_%H:%M:%S')

echo "Start export misure process..."

spark2-submit \
    --class it.eng.au.portale_consumi_ee.Main \
    --master yarn \
    --num-executors ${spark.num.executors} \
    --executor-cores ${spark.executor.cores} \
    --executor-memory ${spark.executor.memory} \
    --driver-cores ${spark.driver.cores} \
    --driver-memory ${spark.driver.memory} \
    --conf spark.executor.memoryOverhead=4g \
    --conf spark.dynamicAllocation.enabled=${spark.dynamicAllocation.enabled} \
    --conf spark.dynamicAllocation.minExecutors=${spark.dynamicAllocation.minExecutors} \
    --conf spark.dynamicAllocation.maxExecutors=${spark.dynamicAllocation.maxExecutors} \
    --conf spark.dynamicAllocation.initialExecutors=${spark.dynamicAllocation.initialExecutors} \
    --files file:${deploy.path.export_misure}/conf/log4j-export_misure.properties,file:${deploy.path.export_misure}/conf/application.conf \
    --conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration.file=${deploy.path.export_misure}/conf/log4j-export_misure.properties -Dconfig.file=${deploy.path.export_misure}/conf/application.conf -DcurrentDate=$DATE" \
    --conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=application.conf -DcurrentDate=$DATE" \
    --driver-java-options -Dconfig.file=${deploy.path.export_misure}/conf/application.conf \
     ${deploy.path.export_misure}/export_misure.jar "$@"