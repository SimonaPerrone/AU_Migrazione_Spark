spark2-submit \
--class it.eng.au.freezerPreCalcolo.Driver \
--master yarn \
--deploy-mode client \
--num-executors 16 \
--executor-cores 3 \
--executor-memory 21G \
--driver-cores 3 \
--driver-memory 21G \
--files ${isilon.deploy.path}/log4j.properties \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.executor.memoryOverhead=4096 \
${isilon.deploy.path}/freezer_pre_calcolo.jar ${hdfs.deploy.path}/params.properties