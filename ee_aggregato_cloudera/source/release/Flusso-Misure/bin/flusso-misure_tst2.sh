#!/bin/bash
#chmod 755
#

#export HADOOP_USER_NAME=ec2-user

NUM_EXEC=40
NUM_EXEC_CORE=4
NUM_EXEC_MEM=16g
DRIVER_CORES=4
DRIVER_MEMORY=16g
FILES=../conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:../conf/log4j.properties"
CONF2=spark.executor.extraJavaOptions="-Xmn1g -XX:+UseParallelGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution"
CLASS=it.au.misure.cli.FlussoMisureTool
JAR=flusso-misure-prodv2.jar
JDBC_DRIVER=ojdbc7-12.7.0.jar:ImpalaJDBC41.jar
JDBC_DRIVER2=ojdbc7-12.7.0.jar,ImpalaJDBC41.jar
AUTO=0

if [ $AUTO -eq 1 ]
then
echo "auto"
spark-submit \
--driver-class-path $JDBC_DRIVER \
--jars $JDBC_DRIVER2 \
--conf $CONF \
--conf spark.sql.shuffle.partitions=600 \
--conf spark.default.parallelism=600 \
--files $FILES \
--class $CLASS \
$JAR $*
else
spark-submit \
--driver-class-path $JDBC_DRIVER \
--num-executors $NUM_EXEC \
--executor-cores $NUM_EXEC_CORE \
--executor-memory $NUM_EXEC_MEM \
--driver-cores $DRIVER_CORES \
--driver-memory $DRIVER_MEMORY \
--jars $JDBC_DRIVER2 \
--conf $CONF \
--conf spark.sql.shuffle.partitions=600 \
--conf spark.default.parallelism=600 \
--conf spark.yarn.executor.memoryOverhead=5120 \
--files $FILES \
--class $CLASS \
$JAR $* 
fi

