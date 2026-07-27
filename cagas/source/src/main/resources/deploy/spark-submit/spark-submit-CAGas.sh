spark-submit \
--class it.sferanet.au.App \
--master yarn \
--deploy-mode client \
--num-executors 60 \
--executor-cores 3 \
--executor-memory 30G \
--driver-memory 30G \
--driver-cores 3 \
--files ${isilon.deploy.path}/log4j.properties \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
${isilon.deploy.path}/CAGas-1.0-SNAPSHOT.jar ${hdfs.deploy.path}/config.properties