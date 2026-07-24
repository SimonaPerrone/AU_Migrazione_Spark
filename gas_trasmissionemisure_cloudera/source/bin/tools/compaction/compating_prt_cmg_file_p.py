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

#./compating_v3.sh cmg_gas prt_cmg_tml_p_compact /user/silvia/au/misure_gas_au/cmg_gas annomese=202001
def main(argv):
    mode_write = "overwrite"
    database = argv[0] #cmg_gas
    table_name = argv[1] #prt_cmg_file_backeted_p_compact
    path_hdfs_new=argv[2] #/user/silvia/au/misure_gas_au/cmg_gas
    size_partition = argv[3] #123
    year = argv[4] #2020
    month = argv[5] #03
    flusso = argv[6] #TGL

    path_hdfs_src = "{}/{}/t_anno_caricamento={}/t_mese_caricamento={}/t_tipo_servizio={}".format(path_hdfs_new, table_name, year, month, flusso)
    path_hdfs_dest = "{}/{}/t_anno_caricamento={}/t_mese_caricamento={}/t_tipo_servizio={}_tmp".format(path_hdfs_new, table_name, year, month, flusso)

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

    print("Normalize Columns")
    df1 = normalize_columns(df1)

    print("path hdfs dest:{}".format(path_hdfs_dest))

    N = get_repartition_factor(sc,size_partition)
    print("Number Repartition: {}".format(N))
    df1.repartition(N).write.parquet(path_hdfs_dest,mode_write)

    cmd ="ALTER TABLE {}.{} DROP PARTITION (t_anno_caricamento='{}',t_mese_caricamento='{}',t_tipo_servizio='{}')".format(database, table_name, year, month, flusso)
    print("Command: {}".format(cmd))
    sqlCtx.sql(cmd)

    remove_partition(path_hdfs_src)
    rename_partition(path_hdfs_dest, path_hdfs_src)

    sql_cmd = "Msck repair table {}.{}".format(database, table_name)
    print(sql_cmd)
    sqlCtx.sql(sql_cmd)

if __name__ == "__main__":
    main(sys.argv[1:])

