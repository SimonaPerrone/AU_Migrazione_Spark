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


databaseProd = "au"

class Commit:
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

        if self.CheckTable(sqlCtx,"au", "gas_tal_500")   == 1:      self.commitTable(sqlCtx, "commit_TAL.sql", "gas_tal_500")
        if self.CheckTable(sqlCtx,"au", "gas_rml_55")    == 1:      self.commitTable(sqlCtx, "commit_RML.sql", "gas_rml_55")
        if self.CheckTable(sqlCtx,"au", "gas_rmv")       == 1:      self.commitTable(sqlCtx, "commit_RMV.sql", "gas_rmv")
        if self.CheckTable(sqlCtx,"au", "gas_rsl")       == 1:      self.commitTable(sqlCtx, "commit_RSL.sql", "gas_rsl")
        if self.CheckTable(sqlCtx,"au", "gas_sw1_50")    == 1:      self.commitTable(sqlCtx, "commit_SW1.sql", "gas_sw1_50")
        if self.CheckTable(sqlCtx,"au", "gas_tas")       == 1:      self.commitTable(sqlCtx, "commit_TAS.sql", "gas_tas")
        if self.CheckTable(sqlCtx,"au", "gas_tav")       == 1:      self.commitTable(sqlCtx, "commit_TAV.sql", "gas_tav")
        if self.CheckTable(sqlCtx,"au", "gas_tgl_50")    == 1:      self.commitTable(sqlCtx, "commit_TGL.sql", "gas_tgl_50")
        if self.CheckTable(sqlCtx,"au", "gas_tml")       == 1:      self.commitTable(sqlCtx, "commit_TML.sql", "gas_tml")
        if self.CheckTable(sqlCtx,"au", "gas_rgl_55")    == 1:      self.commitTable(sqlCtx, "commit_RGL.sql", "gas_rgl_55")

        return


    def PrintError(self, message):
        print ("Error: " + message)

    def CheckTable(self, sqlCtx, database, nameTable):
        return sqlCtx.sql("show tables in " + database).where(col("tableName") == nameTable).count()

    def commitTable(self, sqlCtx,  nameFileSQL, nameTable):
        pathFileSQL = os.path.dirname(os.path.realpath(__file__)) + "/SQL/"
        with open(pathFileSQL + nameFileSQL, 'r') as fileSQL:
            data = fileSQL.read()

        print (data)
        sqlCtx.sql(data)
        sqlCtx.sql("MSCK REPAIR TABLE " + databaseProd + "." + nameTable )

if __name__ == "__main__":
    Commit("local").start()

