package it.eng.au.aggregatoreConsumiCommon.utility

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.util.Properties

trait SparkImplicit {
  @transient var _sc: SparkContext = _
  @transient var _sqlContext: SQLContext = _
  @transient var _fs: FileSystem = _
  implicit lazy val sc: SparkContext = _sc
  implicit lazy val sqlContext: SQLContext = _sqlContext
  implicit lazy val fs: FileSystem = _fs
  implicit val jobProperties: Properties = new Properties()

  def initialize(appName: String = "AggregatoreConsumiAggiustamento", jobPropertiesPath: String): Unit = {
    val sparkSession = SparkSession.builder
      .appName(appName)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    //
    //    val conf = new SparkConf().setAppName(appName)
    //    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    //    conf.set("spark.sql.hive.convertMetastoreParquet", "false")

    _sc = sparkSession.sparkContext
    _sqlContext = sparkSession.sqlContext

    _fs = FileSystem.get(_sc.hadoopConfiguration)

    jobProperties.load(fs.open(new Path(jobPropertiesPath)))

    if(!jobProperties.getProperty("output.file.couples").toUpperCase.contains("DETTAGLIOUNICO")) {
      _sc.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          FileUtility.setYarn777toTmpFolder()
          super.onApplicationEnd(applicationEnd)
        }
      })
    }

  }

}
