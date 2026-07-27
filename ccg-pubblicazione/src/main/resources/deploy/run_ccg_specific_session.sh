DATE=$(date +'%Y-%m-%d_%H:%M:%S')

DRIVER_CORE=3
DRIVER_MEMORY=15G
NUM_EXECUTOR=20
EXECUTOR_CORES=3
EXECUTOR_MEMORY=15G
EXECUTOR_MEMORYOVERHEAD=3G
TIPO="ALL"

while getopts ":s:t:r:n:c:m:x:d:e:h:" opt; do
  case $opt in
    s) flowTypeName="$OPTARG"
    ;;
    t) TIPO="${OPTARG:-$TIPO}"
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
    e) DATA_RICHIESTA="$OPTARG"
    ;;
    h) DATE="${OPTARG:-$DATE}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

#TODO fare un test execution id vuoto
spark2-submit \
--class it.eng.au.ccgPubblicazione.Driver \
--master yarn \
--deploy-mode client \
--num-executors ${NUM_EXECUTOR} \
--executor-cores ${EXECUTOR_CORES} \
--executor-memory ${EXECUTOR_MEMORY} \
--driver-memory ${DRIVER_MEMORY} \
--driver-cores ${DRIVER_CORE} \
--conf spark.executor.memoryOverhead=${EXECUTOR_MEMORYOVERHEAD} \
--files ${deploy.path}/log4j.properties \
--conf "spark.driver.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path}/log4j.properties -DflowTypeName=$flowTypeName -Dtipo=$TIPO -DcurrentDate=$DATE" \
--conf "spark.executor.extraJavaOptions=-XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path}/log4j.properties -DflowTypeName=$flowTypeName -Dtipo=$TIPO -DcurrentDate=$DATE" \
${deploy.path}/ccg-pubblicazione.jar -p ${deploy.path.hdfs}/params.properties -s $flowTypeName -t $TIPO -e $DATA_RICHIESTA