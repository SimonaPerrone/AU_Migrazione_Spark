session="CDP"
DRIVER_CORE=4
DRIVER_MEMORY=16G
NUM_EXECUTOR=8
EXECUTOR_CORES=3
EXECUTOR_MEMORY=12G
EXECUTOR_MEMORYOVERHEAD=4096
PROPERTIES=${hdfs.deploy.path}/params.properties

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
--class it.eng.au.freezerPreCalcolo.Driver \
--master yarn \
--deploy-mode client \
--num-executors $NUM_EXECUTOR \
--executor-cores $EXECUTOR_CORES \
--executor-memory $EXECUTOR_MEMORY \
--driver-cores $DRIVER_CORE \
--driver-memory $DRIVER_MEMORY \
--files ${isilon.deploy.path}/log4j.properties \
--conf spark.executor.memoryOverhead=4096 \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${isilon.deploy.path}/log4j.properties" \
${isilon.deploy.path}/freezer_pre_calcolo.jar -p $PROPERTIES -s $session $SYS_DATE
