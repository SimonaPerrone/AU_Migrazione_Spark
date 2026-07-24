echo "run msck repair table initial"
${root.path.files}/msck_repair_table.sh

spark2-submit \
--class it.eng.au.partitionOptimization.Driver \
--master yarn \
--deploy-mode client \
--executor-cores 3 \
--executor-memory 21G \
--driver-memory 21G \
--driver-cores 3 \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=40 \
--conf spark.dynamicAllocation.initialExecutors=2 \
--files ${root.path.files}/log4j.properties \
--conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${root.path.files}/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${root.path.files}/log4j.properties" \
${root.path.files}/partition-optimization-repartition.jar ${path.properties}/params.properties

echo "run msck repair table"
${root.path.files}/msck_repair_table.sh