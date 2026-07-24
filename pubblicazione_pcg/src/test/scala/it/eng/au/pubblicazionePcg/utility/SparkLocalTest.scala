package it.eng.au.pubblicazionePcg.utility

import junit.framework.TestCase
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}

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

  def initialize(appName: String = "TestPubblicazionePcg",
                 jobPropertiesPath: String = "src/test/resources/deploy/params.properties"): Unit = {

    jobProperties.load(new FileInputStream(new File(jobPropertiesPath)))

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "2")

    _sc = SparkContext.getOrCreate(conf)
    _sqlContext = new SQLContext(_sc)
    _fs = FileSystem.get(_sc.hadoopConfiguration)
  }

  override def setUp(): Unit = {
    initialize()
  }
}
