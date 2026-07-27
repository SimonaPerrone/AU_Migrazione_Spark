package it.au.misure.ee_switching.utility

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkImplicit {
  @transient var _sc: SparkContext = _
  @transient var _sqlContext: SQLContext = _
  @transient var _hiveContext: HiveContext = _

  implicit lazy val sc: SparkContext = _sc
  implicit lazy val hiveContext: HiveContext = _hiveContext

  def initializeSpark(appName: String): Unit = {
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.sql.shuffle.partitions", "2000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    conf.registerKryoClasses(Array(classOf[Row]))

    _sc = SparkContext.getOrCreate(conf)

    _sqlContext = SQLContext.getOrCreate(_sc)

    _hiveContext = new HiveContext(sc)
    _hiveContext.setConf("hive.exec.dynamic.partition", "true")
    _hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    _hiveContext.setConf("spark.sql.sources.partitionOverwriteMode", "dynamic")
  }
}
