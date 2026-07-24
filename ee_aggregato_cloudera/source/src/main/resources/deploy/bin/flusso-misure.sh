#!/bin/bash
#chmod 755
#

#export HADOOP_USER_NAME=ec2-user

NUM_EXEC=${spark.num.executors}
NUM_EXEC_CORE=${spark.executor.cores}
NUM_EXEC_MEM=${spark.executor.memory}
DRIVER_CORES=${spark.driver.cores}
DRIVER_MEMORY=${spark.driver.memory}


DEPLOY_PATH=${deploy.path}
DEPLOY_PATH_OLD=/apps/deploy
FILES=file:${DEPLOY_PATH}/conf/log4j.properties#log4j.properties,${DEPLOY_PATH}/conf/application.conf#conf/application.conf,${DEPLOY_PATH}/conf/ammissibilita.conf#conf/ammissibilita.conf

CLASS=it.au.misure.cli.FlussoMisureTool
JAR=${DEPLOY_PATH}/bin/flusso-misure-2.0.jar
JDBC_DRIVER=${DEPLOY_PATH}/bin/ojdbc7-12.7.0.jar:${DEPLOY_PATH}/bin/ImpalaJDBC41.jar
JDBC_DRIVER2=${DEPLOY_PATH}/bin/ojdbc7-12.7.0.jar,${DEPLOY_PATH}/bin/ImpalaJDBC41.jar


spark-submit \
--driver-class-path $JDBC_DRIVER \
--num-executors $NUM_EXEC \
--executor-cores $NUM_EXEC_CORE \
--executor-memory $NUM_EXEC_MEM \
--driver-cores $DRIVER_CORES \
--driver-memory $DRIVER_MEMORY \
--jars $JDBC_DRIVER2 \
--files $FILES \
--driver-java-options -Dconfig.file=${DEPLOY_PATH}/conf/application.conf \
--conf "spark.driver.extraJavaOptions= -DPROP_FILE=${DEPLOY_PATH_OLD}/job.properties -Dconfig.file=${DEPLOY_PATH}/conf/application.conf -Dlog4j.configuration=file:${DEPLOY_PATH}/conf/log4j.properties " \
--conf "spark.executor.extraJavaOptions= -Dconfig.file=conf/application.conf" \
--class $CLASS \
$JAR $*
