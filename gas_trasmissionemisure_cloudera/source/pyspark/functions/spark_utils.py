
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
import xml.etree.ElementTree as ET
from pyspark.sql.types import *


def set_spark_context(app_name, master):
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
    
        except:
            sqlCtx = sqlContext = SQLContext(sc)
    
    
        sc._jsc.hadoopConfiguration().set( "mapreduce.input.fileinputformat.input.dir.recursive", "true")
        sqlContext.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        sqlContext.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        sqlContext.setConf("spark.scheduler.mode", "FAIR")
        #sqlContext.setConf("spark.executor.instances", "5")
	
        return sc, sqlContext