package it.eng.au.aggregatoreConsumiCommon

import junit.framework.TestCase
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.io.{File, FileInputStream}
import java.util.Properties

trait SparkLocalTest extends TestCase {
  @transient var _sc: SparkContext = _
  @transient var _sqlContext: SQLContext = _
  @transient var _fs: FileSystem = _
  implicit lazy val sc: SparkContext = _sc
  implicit lazy val sqlContext: SQLContext = _sqlContext
  implicit lazy val fs: FileSystem = _fs
  implicit val jobProperties: Properties = new Properties()

  def initialize(appName: String = "TestAggregatoreConsumiAggiustamento",
                 jobPropertiesPath: String = "src/test/resources/params.properties"): Unit = {

    jobProperties.load(new FileInputStream(new File(jobPropertiesPath)))

    val sparkSession = SparkSession.builder
      .appName("Test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    //    val conf = new SparkConf()
    //      .setAppName(appName)
    //      .setMaster("local[*]")
    //      .set("spark.sql.shuffle.partitions", "2")

    _sc = sparkSession.sparkContext
    _sqlContext = sparkSession.sqlContext
    _fs = FileSystem.get(_sc.hadoopConfiguration)
  }

  override def setUp(): Unit = {
    initialize()
  }
}
