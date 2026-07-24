#!/bin/bash
deploy_path=${deploy.path}
DATE=$(date +'%Y-%m-%d_%H:%M:%S')

echo "Running Unzip process..."

spark2-submit \
    --class it.au.misure.ingestionMisureGasUnico.driver.UnzipDriver \
    --master yarn \
    --deploy-mode ${deploy.mode} \
    --num-executors ${spark.num.executors} \
    --executor-cores ${spark.executor.cores} \
    --executor-memory ${spark.executor.memory} \
    --driver-memory ${spark.driver.memory} \
    --files file:${deploy_path}/conf/log4j-unzip.properties#conf/log4j-unzip.properties,${deploy_path}/conf/application.conf#conf/application.conf,${deploy_path}/conf/ammissibilita.conf#conf/ammissibilita.conf \
    --driver-java-options -Dconfig.file=${deploy_path}/conf/application.conf \
    --conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=${deploy_path}/conf/application.conf -Dlog4j.configuration=file:${deploy_path}/conf/log4j-unzip.properties -DcurrentDate=$DATE" \
    --conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dconfig.file=conf/application.conf -DcurrentDate=$DATE" \
    ${deploy_path}/ingestion-misure-gas-unico.jar "$@"