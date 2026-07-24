import sys
import math
import os.path
from os import path
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

def usage():
    print("...")

def main(argv):

    print(argv)
    if len(argv) != 3:
        sys.stderr.write("Error Arg {}".format(len(argv)))
        usage()
        return

    database = argv[0]
    table_name = argv[1]
    table_name_dest = "{}_compact".format(argv[1])
    path_hdfs = argv[2]
    filename_partitions = argv[3]

    if not path.exists(filename_partitions):
        sys.stderr.write("Error file not exist: {}".format(filename_partitions))
        return

    print("database: {}\n"
          "table_name: {}\n"
          "table_name_dest: {}\n"
          "path_hdfs: {}\n"
          "filename_partitions: {}".format(database, table_name, table_name_dest, path_hdfs, filename_partitions))

    app_name = "Compating table: {}".format(table_name)
    mode = "yarn-client"
    #mode = "local"

    print("Create spark context (mode: {})".format(mode))
    sc, sqlCtx = set_spark_context(
        app_name,
        mode)

    i=1
    print("Open file {}".format(filename_partitions))
    with open(filename_partitions) as file_in:
        lines = []
        for line_str in file_in:
            line_2 = line_str.rstrip()
            line = line_2.split(",")[1]

            file = "{}/{}/{}/{}".format(path_hdfs, database, table_name, line)
            path_hdfs_dest = "{}/{}/{}".format(path_hdfs, database, table_name_dest)

            print("Elaboration [{}] file: {}".format(i, file))

            size_partition = int(line_2.split(",")[0])
            if size_partition <= 0:
                print("ERROR: size partition is: {}\tfile parquet: <{}>".format(size_partition, file))

            if size_partition > 0:
                items_partions = line.split("/")
                dict_partions = {}
                partitions = []

                print("Read parquet: <{}>".format(file))
                df1 = sqlCtx.read.parquet(file)

                print("\tAdding partitions")
                for item in items_partions:
                    key = item.split("=")[0]
                    partitions.append(key)
                    value = item.split("=")[1]
                    dict_partions[key] = value
                    print("\tPartition: {}=<{}>".format(key, value))
                    df1 = df1.withColumn(key, lit(value))

                print("\tNormalize columns in StringType")
                for col in df1.columns:
                    df1 = df1.withColumn(col, df1[col].cast(StringType()))

                print("\tRemove errors")
                df1 = df1.withColumn("t_tipo_servizio", lit(normalize_errors(dict_partions["t_tipo_servizio"])) )
                
                print("\tPath hdfs destination:{}".format(path_hdfs_dest))
                mode_write='append'
                N = get_repartition_factor(sc,size_partition)
                print("\tNumber repartition: {}".format(N))

                print("\tWriting...")
                df1.repartition(N).write.partitionBy(partitions).parquet(path_hdfs_dest,mode_write)
                print("\tWrite completed\n")
            i=i+1


    sql_cmd = "Msck repair table {}.{}".format(database, table_name_dest)
    print(sql_cmd)
    sqlCtx.sql(sql_cmd)


if __name__ == "__main__":
    main(sys.argv[1:])


