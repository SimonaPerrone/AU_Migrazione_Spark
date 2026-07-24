package it.au.misure.eng.utility

import junit.framework.TestCase
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkLocal extends TestCase {
  @transient var _sc: SparkContext = _
  @transient var _sqlContext: SQLContext = _

  implicit lazy val sc: SparkContext = _sc
  implicit lazy val sqlContext: SQLContext = _sqlContext

  def initializeSpark(appName: String): Unit = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")

    _sc = SparkContext.getOrCreate(conf)

    _sqlContext = SQLContext.getOrCreate(_sc)
  }

  override def setUp(): Unit = {
    initializeSpark("test")
    super.setUp()
  }
}