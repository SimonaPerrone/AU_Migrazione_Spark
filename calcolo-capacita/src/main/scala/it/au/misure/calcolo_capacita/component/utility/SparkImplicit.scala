package it.au.misure.calcolo_capacita.component.utility

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkImplicit {

  implicit lazy val (sqlContext, hContext) = build("app")

  private def build(appName: String): (SQLContext, HiveContext) = {
    val conf = new SparkConf().setAppName(appName)
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val hContext = new HiveContext(sc)

//    hContext.setConf("hive.exec.dynamic.partition", "true")
//    hContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//    hContext.setConf("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (sqlContext, hContext)
  }
}