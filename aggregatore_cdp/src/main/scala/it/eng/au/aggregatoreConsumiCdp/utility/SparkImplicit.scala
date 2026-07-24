package it.eng.au.aggregatoreConsumiCdp.utility

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

trait SparkImplicit {
  @transient var _sc: SparkContext = _
  @transient var _sqlContext: HiveContext = _
  @transient var _fs: FileSystem = _
  implicit lazy val sc: SparkContext = _sc
  implicit lazy val sqlContext: SQLContext = _sqlContext
  implicit lazy val fs: FileSystem = _fs
  implicit val jobProperties: Properties = new Properties()

  def initialize(appName: String = "[CDP] Aggregatore Consumi CDP", jobPropertiesPath: String): Unit = {
    val conf = new SparkConf().setAppName(appName)
    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    _sc = SparkContext.getOrCreate(conf)
    _sqlContext = new HiveContext(_sc)

    _fs = FileSystem.get(_sc.hadoopConfiguration)

    jobProperties.load(fs.open(new Path(jobPropertiesPath)))

    /*_sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        FileUtility.setYarn777toTmpFolder()
        super.onApplicationEnd(applicationEnd)
      }
    })*/

  }

}
