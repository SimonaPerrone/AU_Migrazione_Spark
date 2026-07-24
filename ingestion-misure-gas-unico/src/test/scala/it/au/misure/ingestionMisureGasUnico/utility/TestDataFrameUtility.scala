package it.au.misure.ingestionMisureGasUnico.utility

import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.apache.spark.sql.Row
import org.junit.Assert

class TestDataFrameUtility extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._

  def testTrimColumns(): Unit = {
    val schema = List("A", "B")
    val df = spark.sparkContext.parallelize(Seq(
      (" 1", "111   ")
    )).toDF(schema: _*)
    val result = DataFrameUtility.trimColumns(df, schema).cache()
    Assert.assertEquals(Row("1", "111"), result.head())
  }

  def testTrimColumns_NullValues(): Unit = {
    val schema = List("A", "B")
    val df = spark.sparkContext.parallelize(Seq(
      (" 1", null)
    )).toDF(schema: _*)
    val result = DataFrameUtility.trimColumns(df, schema).cache()
    Assert.assertEquals(Row("1", null), result.head())
  }
}
