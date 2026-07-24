spark2-submit \
--class it.eng.au.partitionDeleteBackup.Driver \
--master yarn \
--deploy-mode client \
--num-executors 1 \
--executor-cores 1 \
--executor-memory 1G \
--driver-memory 16G \
--driver-cores 8 \
--files ${root.path.files}/log4j.properties \
--conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${root.path.files}/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${root.path.files}/log4j.properties" \
${root.path.files}/partition-optimization-delete-backup.jar ${path.properties}/params.properties