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
    print("Remove directory:{}".format(path))
    res = os.system('hadoop fs -rm -R -skipTrash "{}"'.format(path))
    #print("Remove result:{}".format(res))


def rename_partition(path_src, path_dest):
    #import os
    print("Rename path {} -> {}".format(path_src, path_dest))
    res = os.system('hadoop fs -mv "{}" "{}"'.format(path_src, path_dest))
    #print("Rename result:{}".format(res))

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(int(size_bytes), 1024)))
   p = math.pow(1024, i)
   s = round(int(size_bytes) / p, 2)
   return "%s %s" % (s, size_name[i])


def main(argv):
    mode_write = "overwrite"
    database = argv[0]
    table_name = argv[1]
    path_hdfs_new=argv[2]
    size_partition = argv[3]
    partition = argv[4]
    #filename_partitions = argv[3]

    partition_name = partition.split("=")[0]
    partition_value = partition.split("=")[1]


    path_hdfs_src = "{}/{}/{}".format(path_hdfs_new,table_name, partition)
    path_hdfs_dest = "{}/{}/{}_tmp".format(path_hdfs_new,table_name, partition)

    print("Database: {}\n"
          "Table Name SRC: {}\n"
          "HDFS Path Src: {}\n"
          "Size Partitions: {} bytes ({})".format(database, table_name, path_hdfs_src, size_partition, convert_size(size_partition)))

    app_name = "Compating table: {}".format(table_name)
    mode = "yarn-client"
    sc, sqlCtx = set_spark_context(
        app_name,
        mode)

    file_parquet = "{}".format(path_hdfs_src)
    print("Read parquet: {}".format(file_parquet))
    df1 = sqlCtx.read.parquet(file_parquet)

    #conversione delle colonne in string
    print("Normalize Columns")
    df1 = normalize_columns(df1)

    #calcola il numero di file per la partizione
    print("path hdfs dest:{}".format(path_hdfs_dest))
    N = get_repartition_factor(sc,size_partition)

    #scrive
    print("Number Repartition: {}".format(N))
    df1.repartition(N).write.parquet(path_hdfs_dest,mode_write)

    cmd ="ALTER TABLE {}.{} DROP PARTITION ({}='{}')".format(database, table_name, partition_name, partition_value)
    print("Command: {}".format(cmd))
    sqlCtx.sql(cmd)

    remove_partition(path_hdfs_src)
    rename_partition(path_hdfs_dest, path_hdfs_src)


    sql_cmd = "Msck repair table {}.{}".format(database, table_name)
    print(sql_cmd)
    sqlCtx.sql(sql_cmd)

if __name__ == "__main__":
    main(sys.argv[1:])


