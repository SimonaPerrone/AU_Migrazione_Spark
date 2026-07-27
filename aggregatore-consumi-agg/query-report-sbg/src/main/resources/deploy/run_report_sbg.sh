#!/bin/bash

spark2-submit \
--class it.eng.au.queryReport.Driver \
--master yarn \
--deploy-mode client \
--num-executors 40 \
--executor-cores 3 \
--executor-memory 15G \
--driver-memory 15G \
--driver-cores 3 \
--files ${deploy.path}/log4j.properties \
--conf spark.executor.memoryOverhead=4096 \
--conf spark.sql.shuffle.partitions=600 \
--conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path}/log4j.properties"  \
--conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path}/log4j.properties" \
${deploy.path}/query-report-sbg.jar ${hdfs.config.path}/params.properties