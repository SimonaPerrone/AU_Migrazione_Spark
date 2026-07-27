spark2-submit \
--class it.eng.cdp_codprofstd_tds.Driver \
--master yarn \
--deploy-mode client \
--num-executors 8 \
--executor-cores 3 \
--executor-memory 12G \
--driver-cores 4 \
--driver-memory 16G \
--files ${isilon.deploy.path}/log4j.properties \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.executor.memoryOverhead=4096 \
${isilon.deploy.path}/cdp-codprofstd-tds.jar -p ${deploy.path.hdfs}/params.properties