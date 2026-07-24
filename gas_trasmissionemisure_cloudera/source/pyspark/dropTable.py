import logging
import os
import constants

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import expr
from pyspark.sql import HiveContext
import xml.etree.ElementTree as ET
from pyspark.sql.types import *
from pyspark.sql.functions import lit
import datetime

from Data import Data
from validator import Validator
from functions.util_file import UtilFiles
import threading


from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType

class DropTable:
    def __init__(self, mode):
        self.master = mode
        self.appName = constants.APPNAME



    def set_spark_context(self, app_name, master):
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
        sqlContext.setConf("spark.scheduler.mode", "FAIR")

        sqlContext.sql("set hive.exec.dynamic.partition=true")
        sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        sqlContext.sql("set hive.mapred.mode = nonstrict")
        sqlContext.sql("set hive.exec.parallel=true")
	
        return sc, sqlContext

    def start(self):
        sc, sqlCtx = self.set_spark_context(self.appName, self.master)

        self.dropTable(sqlCtx, "drop_TAL.sql")
        self.dropTable(sqlCtx, "drop_RML.sql")
        self.dropTable(sqlCtx, "drop_RMV.sql")
        self.dropTable(sqlCtx, "drop_RSL.sql")
        self.dropTable(sqlCtx, "drop_SW1.sql")
        self.dropTable(sqlCtx, "drop_RGL.sql")
        self.dropTable(sqlCtx, "drop_TAS.sql")
        self.dropTable(sqlCtx, "drop_TAV.sql")
        self.dropTable(sqlCtx, "drop_TGL.sql")
        self.dropTable(sqlCtx, "drop_TML.sql")
        self.dropTable(sqlCtx, "drop_RGL.sql")

        self.dropTable(sqlCtx, "drop_gas_ca_AltreFreq.sql")
        self.dropTable(sqlCtx, "drop_gas_ca_mensili.sql")
        self.dropTable(sqlCtx, "drop_gas_categtermica.sql")
        self.dropTable(sqlCtx, "drop_gas_classiprelievo.sql")
        self.dropTable(sqlCtx, "drop_gas_error_parsed.sql")
        self.dropTable(sqlCtx, "drop_gas_param_carat.sql")
        self.dropTable(sqlCtx, "drop_gas_profstand.sql")
        self.dropTable(sqlCtx, "drop_gas_wk.sql")


        return


    def PrintError(self, message):
        print ("Error: " + message)

    
    def dropTable(self, sqlCtx,  nameFileSQL):
        pathFileSQL = os.path.dirname(os.path.realpath(__file__)) + "/SQL/"
        with open(pathFileSQL + nameFileSQL, 'r') as fileSQL:
            data = fileSQL.read()

        print (data)
        sqlCtx.sql(data)

if __name__ == "__main__":
    DropTable("local").start()

