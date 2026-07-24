PROPERTIES=${hdfs.deploy.path}/params.properties

while getopts ":p:" opt; do
  case $opt in
    p) PROPERTIES="${OPTARG:-$PROPERTIES}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

spark2-submit \
--class it.eng.au.aggregatoreConsumiCdp.Driver \
--master yarn \
--deploy-mode client \
--driver-cores 4 \
--driver-memory 28G \
--num-executors 40 \
--executor-cores 3 \
--executor-memory 21G \
--files ${isilon.deploy.path}/log4j.properties \
--conf spark.executor.memoryOverhead=4096 \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf spark.sql.shuffle.partitions=2000 \
${isilon.deploy.path}/aggregatore_cdp.jar $PROPERTIES

echo "Emptying temporary folder.."
rm -rf ${isilon.basepath.tmp}/tmp/CDP/CDP1/*
rm -rf ${isilon.basepath.tmp}/tmp/CDP/CDP2/*
rm -rf ${isilon.basepath.tmp}/tmp/CDP/CDP3/*
echo "Temporary folder emptied."