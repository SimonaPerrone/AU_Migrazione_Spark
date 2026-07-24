import sys
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, udf, col

def set_spark_context(app_name, master):
    """
    Crea il context spark
    """
    conf = SparkConf().setAppName(app_name) \
        .setMaster(master) \
        .set("spark.shuffle.service.enabled", "false") \
        .set("spark.dynamicAllocation.enabled", "false") \
        .set("spark.io.compression.codec", "snappy") \
        .set("spark.rdd.compress", "true") \
        .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
        .set("spark.sql.execution.arrow.enabled", "true")
    sc = SparkContext(conf=conf)
    try:
        sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        sqlCtx = sqlContext = HiveContext(sc)

    except py4j.protocol.Py4JError:
        sqlCtx = sqlContext = SQLContext(sc)


    sc._jsc.hadoopConfiguration().set( "mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sqlContext.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    sqlContext.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("hive.exec.max.dynamic.partitions", "1000000000")

    sqlContext.setConf("spark.scheduler.mode", "FAIR")
    #sqlContext.setConf("spark.executor.instances", "5")
    
    return sc, sqlContext

def main(argv):
    app_name = "Compating Procedure"
    mode="yarn-client"
    sc, sqlCtx = set_spark_context(
        app_name,
        mode)

    database = argv[0]
    table_name = argv[1]
    filename_partitions = argv[2]
    number_total_files = int(argv[3])


### TEST ###
    """
    file = "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_sw1_p"
    df = sqlCtx.read.parquet(file)
    df2 = df.withColumn("compacted", lit(False))
    df2.write.partitionBy(["compacted","annomese"]).parquet("/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_sw1_test_c",'append')
    sql_cmd = "Msck repair table {}.{}".format(database, table_name)
    sqlCtx.sql(sql_cmd)
    return 
    """
### TEST ###

    if not number_total_files:
        number_total_files = 10 # Default total number file
    

    with open(filename_partitions) as file_in:
        lines = []
        for line_str in file_in:
            line = line_str.rstrip()
            file = "/user/silvia/au/misure_gas_au/{}/{}/{}".format(database, table_name, line)
            path_hdfs_dest = "/user/silvia/au/misure_gas_au/{}/{}".format(database, table_name)

            print("Read parquet: {}".format(file))
            df1 = sqlCtx.read.parquet(file)

            items_partions=line.split("/")
            compacted_def = items_partions[0].split("=")[-1] == "true"

            dict_partions = {}
            if not compacted_def:
                for item in items_partions:
                    key = item.split("=")[0]
                    value = item.split("=")[1]
                    dict_partions[key] = value
                    df1 = df1.withColumn(key, lit(value))
                
                df1 = df1.withColumn("compacted", lit(True))

                df1.coalesce(number_total_files).write.partitionBy(["compacted","annomese"]).parquet(path_hdfs_dest,'append')
                sql_cmd = "Msck repair table {}.{}".format(database, table_name)
                sqlCtx.sql(sql_cmd)

                partition_str = ", ".join("{}={}".format(k, v) for k, v in dict_partions.items())
                sql_cmd = "ALTER TABLE {}.{} DROP IF EXISTS PARTITION ({})".format(database, table_name, partition_str)
                print (sql_cmd)
                sqlCtx.sql(sql_cmd)

    sql_cmd = "Msck repair table {}.{}".format(database, table_name)
    sqlCtx.sql(sql_cmd)


if __name__ == "__main__":
    main(sys.argv[1:])


"""
#table=prt_cmg_sw1_test_c
#hive -e "show partitions cmg_gas.${table}" 2> /dev/null 1> /tmp/${table}_partitions_tmp.dat
#cat /tmp/${table}_partitions_tmp.dat | grep -v "^WARN" > /tmp/${table}_partitions.dat

PATH_APP=/home/acutest/GAS/
FILES=$PATH_APP/conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/acutest/GAS/conf/log4j.properties"
NUM_EXEC=10
NUM_EXEC_CORE=10
NUM_EXEC_MEM=50g
DRIVER_CORES=5
DRIVER_MEMORY=40g
spark-submit --conf $CONF --files $FILES --num-executors $NUM_EXEC --executor-cores $NUM_EXEC_CORE --executor-memory $NUM_EXEC_MEM --driver-cores $DRIVER_CORES --driver-memory $DRIVER_MEMORY compating.py cmg_gas prt_cmg_sw1_test_c /tmp/prt_cmg_sw1_test_c_partitions.dat 10
"""
