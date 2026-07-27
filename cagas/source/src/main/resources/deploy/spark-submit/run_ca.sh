session="CDP"
DRIVER_CORE=6
DRIVER_MEMORY=45G
NUM_EXECUTOR=30
EXECUTOR_CORES=6
EXECUTOR_MEMORY=45G
EXECUTOR_MEMORYOVERHEAD=4096
PROPERTIES=${hdfs.deploy.path}/config.properties

while getopts ":s:e:d:r:n:c:m:x:p:" opt; do
  case $opt in
    s) session="${OPTARG:-$session}"
    ;;
    e) SYS_DATE="-d ${OPTARG}"
    ;;
    d) DRIVER_CORE="${OPTARG:-$DRIVER_CORE}"
    ;;
    r) DRIVER_MEMORY="${OPTARG:-$DRIVER_MEMORY}"
    ;;
    n) NUM_EXECUTOR="${OPTARG:-$NUM_EXECUTOR}"
    ;;
    c) EXECUTOR_CORES="${OPTARG:-$EXECUTOR_CORES}"
    ;;
    m) EXECUTOR_MEMORY="${OPTARG:-$EXECUTOR_MEMORY}"
    ;;
    x) EXECUTOR_MEMORYOVERHEAD="${OPTARG:-$EXECUTOR_MEMORYOVERHEAD}"
    ;;
    p) PROPERTIES="${OPTARG:-$PROPERTIES}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

spark2-submit \
--class it.sferanet.au.App \
--master yarn \
--deploy-mode client \
--executor-cores $EXECUTOR_CORES \
--executor-memory $EXECUTOR_MEMORY \
--driver-memory $DRIVER_MEMORY \
--driver-cores $DRIVER_CORE \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=$NUM_EXECUTOR \
--conf spark.dynamicAllocation.initialExecutors=4 \
--files ${isilon.deploy.path}/log4j.properties \
--conf spark.executor.memoryOverhead=$EXECUTOR_MEMORYOVERHEAD \
--conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
${isilon.deploy.path}/CAGas.jar -p $PROPERTIES -s $session $SYS_DATE
