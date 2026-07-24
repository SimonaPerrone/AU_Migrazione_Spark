import sys
import math
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, udf, col
from pyspark.sql.types import *

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

    sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sqlContext.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    sqlContext.setConf("spark.sql.parquet.output.committer.class",
                       "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("hive.exec.max.dynamic.partitions", "1000000000")

    sqlContext.setConf("spark.scheduler.mode", "FAIR")
    # sqlContext.setConf("spark.executor.instances", "5")

    return sc, sqlContext

def get_repartition_factor(sc, dir_size):
    block_size =int( sc._jsc.hadoopConfiguration().get("dfs.blocksize"))
    return int(math.ceil(dir_size/block_size))+1 # returns 2


def normalize_errors(t_servizio):
    flussi_arr=["TAV","TMV","RMV","TAL","SW1","RGL","RSL","RML","TML","TGL","TAS","DEF","FUI"]
    print("normalize: {} in flussi:{}".format(t_servizio, (t_servizio in flussi_arr)))
    if t_servizio in flussi_arr:
        return t_servizio
    return "EE"

def main(argv):

    print(argv)
    database = argv[0]
    table_name = argv[1]
    table_name_dest = "{}_compact".format(argv[1])
    path_hdfs = argv[2]
    filename_partitions = argv[3]

    print("database: {}\n"
          "table_name: {}\n"
          "table_name_dest: {}\n"
          "path_hdfs: {}\n"
          "filename_partitions: {}".format(database, table_name, table_name_dest, path_hdfs, filename_partitions))

    app_name = "Compating table: {}".format(table_name)
    #mode = "yarn-client"
    mode = "local"
    sc, sqlCtx = set_spark_context(
        app_name,
        mode)


    with open(filename_partitions) as file_in:
        lines = []
        for line_str in file_in:
            line_2 = line_str.rstrip()
            line = line_2.split(",")[1]

            file = "{}/{}/{}/{}".format(path_hdfs, database, table_name, line)
            path_hdfs_dest = "{}/{}/{}".format(path_hdfs, database, table_name_dest)

            size_partition = int(line_2.split(",")[0])
            if size_partition > 0:
                print("Read parquet: {}".format(file))
                df1 = sqlCtx.read.parquet(file)
                #df1 = sqlCtx.read.csv(file)

                items_partions = line.split("/")

                print("size partions:{}".format(size_partition))
               
                dict_partions = {}
                compacted_def = False
                partitions = []

                if not compacted_def:
                    for item in items_partions:
                       key = item.split("=")[0]
                       partitions.append(key)
                       value = item.split("=")[1]
                       dict_partions[key] = value
                       df1 = df1.withColumn(key, lit(value))

                    for col in df1.columns:
                       df1 = df1.withColumn(col, df1[col].cast(StringType()))
                    
                    #flusso_udf =  udf(lambda f: f if f in flussi_arr else "EE", StringType())
                    #df1 = df1.withColumn("t_tipo_servizio", lit(normalize_errors(dict_partions["t_tipo_servizio"])) )
                    #df1 = df1.withColumn("annomese", lit(dict_partions["annomese"]))
		    #df1.show(truncate = False)
                    #print(df1.schema)
                    #return
                    #print(df1.schema)

		    print("path hdfs dest:{}".format(path_hdfs_dest))
                    #df1.coalesce(number_total_files).write.partitionBy(partitions).parquet(path_hdfs_dest,'append')

                    mode_write='append'
                    #mode_write='overwrite'
                    #df1.coalesce(number_total_files).write.partitionBy(partitions).parquet(path_hdfs_dest,mode_write)
                    N = get_repartition_factor(sc,size_partition)
                    print("repartition: {}".format(N))
                    df1.repartition(N).write.partitionBy(partitions).parquet(path_hdfs_dest,mode_write)

    sql_cmd = "Msck repair table {}.{}".format(database, table_name_dest)
    print(sql_cmd)
    sqlCtx.sql(sql_cmd)


if __name__ == "__main__":
    main(sys.argv[1:])

"""
table=prt_cmg_sw1_test_c
hive -e "show partitions cmg_gas.${table}" 2> /dev/null 1> /tmp/${table}_partitions_tmp.dat
cat /tmp/${table}_partitions_tmp.dat | grep -v "^WARN" > /tmp/${table}_partitions.dat

#hadoop fs -du -s /user/silvia/au/misure_gas_au/cmg_gas/${table}/${partition}

PATH_APP=/home/acutest/GAS/
FILES=$PATH_APP/conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/acutest/GAS/conf/log4j.properties"
NUM_EXEC=10
NUM_EXEC_CORE=10
NUM_EXEC_MEM=50g
DRIVER_CORES=5
DRIVER_MEMORY=40g
spark-submit \
--conf $CONF \
--files $FILES \
--num-executors $NUM_EXEC \
--executor-cores $NUM_EXEC_CORE \
--executor-memory $NUM_EXEC_MEM \
--driver-cores $DRIVER_CORES \
--driver-memory $DRIVER_MEMORY \
compating.py cmg_gas prt_cmg_sw1_test_c /tmp/prt_cmg_sw1_test_c_partitions.dat 10
"""

