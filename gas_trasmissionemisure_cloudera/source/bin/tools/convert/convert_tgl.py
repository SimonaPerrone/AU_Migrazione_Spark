import sys
import os
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
    return int(math.ceil(int(dir_size)/block_size))+1 # returns 2

def normalize_columns(dataframe):
    for col in dataframe.columns:
        dataframe = dataframe.withColumn(col, dataframe[col].cast(StringType()))
    return dataframe


def remove_partition(path):
    #import os
    res = os.system('hadoop fs -rm -R -skipTrash "{}"'.format(path))
    print("Remove result:{}".format(res))


def rename_partition(path_src, path_dest):
    #import os
    res = os.system('hadoop fs -mv "{}" "{}"'.format(path_src, path_dest))
    print("Rename result:{}".format(res))

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(int(size_bytes), 1024)))
   p = math.pow(1024, i)
   s = round(int(size_bytes) / p, 2)
   return "%s %s" % (s, size_name[i])


def main(argv):
    N=1
    filename_partitions="/home/acutest/GAS/bin/tools/data/tgl_mese_comp_partitions4.dat"
    app_name = "Compating table: {}".format("TGL")
    mode = "yarn-client"
    sc, sqlCtx = set_spark_context(
        app_name,
        mode)

    path_hdfs_dest='/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tgl_p_tmp'
    with open(filename_partitions) as file_in:
        lines = []
        for line_str in file_in:
            line_2 = line_str.rstrip()
            line = line_2.split(",")[1]
            size_partition=line_2.split(",")[0]
            file_parquet=line
            print(line)
            partition_name = "mese_comp"
            partition_value = str(line.split("=")[1])
            print("file parquet:{}\npartition_name:{}\npartition_value:{}\nsize:{}".format(file_parquet, partition_name, partition_value,convert_size(size_partition)))
            print("Lettura")
            df1 = sqlCtx.read.parquet(file_parquet)
            print("normalize columns")
            df1 = normalize_columns(df1)
            print("Calculate number repartition")
            N = get_repartition_factor(sc,size_partition)
            print("N:{}".format(N))
            df1 = df1.withColumn(partition_name, lit(partition_value))
            print("Write to:{}".format(path_hdfs_dest))
            df1.repartition(N).write.partitionBy(["mese_comp"]).parquet(path_hdfs_dest,"append")


if __name__ == "__main__":
    main(sys.argv[1:])

