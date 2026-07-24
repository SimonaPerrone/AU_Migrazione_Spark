import logging
import os
import threading

import constants
import datetime
from validator import Validator
from Data import Data
from functions.util_file import UtilFiles

import xml.etree.ElementTree as ET

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext

from pyspark.sql.functions import *
from pyspark.sql.types import *


class Setup:
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
        #sqlContext.setConf("spark.executor.instances", "5")
	
        return sc, sqlContext

    def start(self):
        sc, sqlCtx = self.set_spark_context(self.appName, self.master)

        self.CreateTable(sqlCtx, "gas_ca_AltreFreq.sql"     , "gas_ca_AltreFreq")
        self.CreateTable(sqlCtx, "gas_ca_mensili.sql"       , "gas_ca_mensili")
        self.CreateTable(sqlCtx, "gas_categtermica.sql"     , "gas_categtermica")
        self.CreateTable(sqlCtx, "gas_classiprelievo.sql"   , "gas_classiprelievo")
        self.CreateTable(sqlCtx, "gas_error_parsed.sql"     , "gas_error_parsed")
        self.CreateTable(sqlCtx, "gas_param_carat.sql"      , "gas_param_carat")
        self.CreateTable(sqlCtx, "gas_profstand.sql"        , "gas_profstand")
        self.CreateTable(sqlCtx, "gas_rgl_55.sql"           , "gas_rgl_55")
        self.CreateTable(sqlCtx, "gas_RML_0055.sql"         , "gas_RML_0055")
        self.CreateTable(sqlCtx, "gas_rmv.sql"              , "gas_rmv")
        self.CreateTable(sqlCtx, "gas_rsl.sql"              , "gas_rsl")
        self.CreateTable(sqlCtx, "gas_sw1_50.sql"           , "gas_sw1_50")
        self.CreateTable(sqlCtx, "gas_tal_500.sql"          , "gas_tal_500")
        self.CreateTable(sqlCtx, "gas_tas.sql"              , "gas_tas")
        self.CreateTable(sqlCtx, "gas_tav.sql"              , "gas_tav")
        self.CreateTable(sqlCtx, "gas_tgl_50.sql"           , "gas_tgl_50")
        self.CreateTable(sqlCtx, "gas_tml.sql"              , "gas_tml")
        self.CreateTable(sqlCtx, "gas_wk.sql"               , "gas_wk")

        if self.CheckTable(sqlCtx,"au_test", "gas_ca_AltreFreq")     == 0:   self.PrintError("gas_ca_AltreFreq -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_ca_mensili")       == 0:   self.PrintError("gas_ca_mensili -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_categtermica")     == 0:   self.PrintError("gas_categtermica -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_classiprelievo")   == 0:   self.PrintError("gas_classiprelievo -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_error_parsed")     == 0:   self.PrintError("gas_error_parsed -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_param_carat")      == 0:   self.PrintError("gas_param_carat -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_profstand")        == 0:   self.PrintError("gas_profstand -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_rgl_55")           == 0:   self.PrintError("gas_rgl_55 -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_RML_0055")         == 0:   self.PrintError("gas_RML_0055 -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_rmv")              == 0:   self.PrintError("gas_rmv -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_rsl")              == 0:   self.PrintError("gas_rsl -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_sw1_50")           == 0:   self.PrintError("gas_sw1_50 -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_tal_500")          == 0:   self.PrintError("gas_tal_500 -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_tas")              == 0:   self.PrintError("gas_tas -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_tav")              == 0:   self.PrintError("gas_tav -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_tgl_50")           == 0:   self.PrintError("gas_tgl_50 -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_tml")              == 0:   self.PrintError("gas_tml -- Table not exists")
        if self.CheckTable(sqlCtx,"au_test", "gas_wk")               == 0:   self.PrintError("gas_wk -- Table not exists")


        if os.path.isfile(constants.FILE_XSD_TAL_0050.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TAL_0050 + " not exists")
        if os.path.isfile(constants.FILE_XSD_TAL_0150.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TAL_0150 + " not exists")
        if os.path.isfile(constants.FILE_XSD_RML_0055.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_RML_0055 + " not exists")
        if os.path.isfile(constants.FILE_XSD_RML_0056.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_RML_0056 + " not exists")
        if os.path.isfile(constants.FILE_XSD_TML_0050.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TML_0050 + " not exists")
        if os.path.isfile(constants.FILE_XSD_TGL_0050.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TGL_0050 + " not exists")
        if os.path.isfile(constants.FILE_XSD_SW1_0351.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_SW1_0351 + " not exists")
        if os.path.isfile(constants.FILE_XSD_SW1_0350.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_SW1_0350 + " not exists")
        if os.path.isfile(constants.FILE_XSD_RGL_0055.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_RGL_0055 + " not exists")
        if os.path.isfile(constants.FILE_XSD_RSL_0400.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_RSL_0400 + " not exists")
        if os.path.isfile(constants.FILE_XSD_TAV_0050.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TAV_0050 + " not exists")
        if os.path.isfile(constants.FILE_XSD_TAV_0150.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TAV_0150 + " not exists")
        if os.path.isfile(constants.FILE_XSD_TAS_0050.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TAS_0050 + " not exists")
        if os.path.isfile(constants.FILE_XSD_TAS_0150.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_TAS_0150 + " not exists")
        if os.path.isfile(constants.FILE_XSD_RMV_0400.replace("file://","")) == False: self.PrintError("File " + constants.FILE_XSD_RMV_0400 + " not exists")
        
        return


    def PrintError(self, message):
        print ("Error: " + message)


    def CreateTable(self, sqlCtx, nameFileSQL, nameTable):
        pathFileSQL = os.path.dirname(os.path.realpath(__file__)) + "/SQL/"
        with open(pathFileSQL + nameFileSQL, 'r') as fileSQL:
            data = fileSQL.read()

        print (data)
        sqlCtx.sql(data)
        sqlCtx.sql("MSCK REPAIR TABLE au_test." + nameTable )


    def CheckTable(self, sqlCtx, database, nameTable):
        return sqlCtx.sql("show tables in " + database).where(col("tableName") == nameTable).count()

if __name__ == "__main__":
    Setup("local").start()

