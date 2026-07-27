DRIVER_CORE=3
DRIVER_MEMORY=30G
NUM_EXECUTOR=60
EXECUTOR_CORES=3
EXECUTOR_MEMORY=30G
EXECUTOR_MEMORYOVERHEAD=10G
SHUFFLE_PARTITION=200
PROPERTIES=${deploy.path.hdfs}/params.properties

while getopts ":d:r:n:c:m:x:h:p:" opt; do
  case $opt in

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
    h) SHUFFLE_PARTITION="${OPTARG:-$SHUFFLE_PARTITION}"
    ;;
    p) PROPERTIES="${OPTARG:-$PROPERTIES}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac

done

spark2-submit \
--class it.eng.au.sgsFlussoStoricoGas.run.pubblicazione.Driver \
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
--files ${deploy.path}/log4j.properties \
--conf "spark.driver.extraJavaOptions=-Xss512m -XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path}/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Xss512m -XX:hashCode=0 -Dlog4j.configuration=file:${deploy.path}/log4j.properties" \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--conf spark.sql.parquet.output.committer.class=org.apache.parquet.hadoop.ParquetOutputCommitter \
--conf spark.sql.sources.commitProtocolClass=org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol \
--conf spark.sql.shuffle.partitions=$SHUFFLE_PARTITION \
--conf spark.executor.memoryOverhead=$EXECUTOR_MEMORYOVERHEAD \
--conf spark.network.timeout=800 \
${deploy.path}/sgs-flusso-storico-gas.jar -p $PROPERTIES