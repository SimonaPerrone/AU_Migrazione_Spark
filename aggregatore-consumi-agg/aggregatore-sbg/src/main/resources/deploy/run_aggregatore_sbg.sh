PROPERTIES=${hdfs.path}/params.properties

while getopts ":o:p:" opt; do
  case $opt in
    o) OUTPUT_FILE_COUPLES="-o ${OPTARG}"
    ;;
    p) PROPERTIES="${OPTARG:-$PROPERTIES}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done


spark2-submit \
--class it.eng.au.aggregatoreConsumiSbg.Driver \
--master yarn \
--deploy-mode client \
--num-executors 20 \
--executor-cores 3 \
--executor-memory 15G \
--driver-memory 15G \
--driver-cores 3 \
--files ${deploy.path}/log4j.properties \
--conf spark.executor.memoryOverhead=4096 \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${deploy.path}/log4j.properties" \
--conf spark.sql.shuffle.partitions=600 \
${deploy.path}/aggregatore-sbg.jar -p $PROPERTIES $OUTPUT_FILE_COUPLES

echo "Emptying temporary folder.."
rm -r ${isilon.basepath.tmp}/tmp/SBG/SBG1/*
rm -r ${isilon.basepath.tmp}/tmp/SBG/SBG2/*
rm -r ${isilon.basepath.tmp}/tmp/SBG/SBG3/*
rm -r ${isilon.basepath.tmp}/tmp/SBG/SBG4/*
rm -r ${isilon.basepath.tmp}/tmp/SBG/SBG5/*
echo "Temporary folder emptied."