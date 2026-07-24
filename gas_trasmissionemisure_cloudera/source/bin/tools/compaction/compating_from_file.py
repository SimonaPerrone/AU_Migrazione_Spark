import sys
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, udf, col
from pyspark.sql.types import *
import os


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

def main(argv):
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
    mode = "yarn-client"
    sc, sqlCtx = set_spark_context(
        app_name,
        mode)


    InputPath = []
    dict_partions = {}
    partitions = []

    #/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_file_backeted_p/t_anno_caricamento=2020/t_mese_caricamento=03/t_tipo_servizio=RGL/part-r-00000-12335c14-7625-4a5d-a819-e4daff01ef1b.parquet
    str_remov = "{}/{}/{}/".format(path_hdfs, database, table_name)
    path_hdfs_dest = "{}/{}/{}".format(path_hdfs, database, table_name_dest)


    with open(filename_partitions) as file_in:
        lines = []
        for line_str in file_in:
            line_2 = line_str.rstrip()
            line_part = os.path.dirname(line_2.split(",")[1]).replace(str_remov, "")
            line_file = os.path.basename(line_2.split(",")[1])
            #print(" ")
            #print("file name: {}".format(line_file))
            #print("dir  name: {}".format(line_part))

            items_partions = line_part.split("/")
            for item in items_partions:
                    key = item.split("=")[0]
                    partitions.append(key)
                    value = item.split("=")[1]
                    dict_partions[key] = value
                    
            file = "{}/{}/{}/{}/{}".format(path_hdfs, database, table_name, line_part, line_file)
            InputPath.append(file)

    df1 = sqlCtx.read.parquet(*InputPath)

    print("partitions:{}".format(dict_partions))
    for key in dict_partions:
        value=dict_partions[key]
        #print("Key:{} value:{},".format(key, value))
        df1 = df1.withColumn(key, lit(value))

    number_total_files = 1
    print("path hdfs dest:{}".format(path_hdfs_dest))
    df1.coalesce(number_total_files).write.partitionBy(partitions).parquet(path_hdfs_dest,'append')

    #df1.show()

    #sql_cmd = "Msck repair table {}.{}".format(database, table_name_dest)
    #print(sql_cmd)
    #sqlCtx.sql(sql_cmd)


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

