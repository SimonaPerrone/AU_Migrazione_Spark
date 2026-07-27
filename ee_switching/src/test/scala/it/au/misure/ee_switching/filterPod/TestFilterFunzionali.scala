package it.au.misure.ee_switching.filterPod

import java.time.LocalDate
import it.au.misure.ee_switching.model.schema.hive.FunzionaliSchema
import it.au.misure.ee_switching.utility.environment.Environment
import it.au.misure.ee_switching.utility.{EnvironmentSparkTest, SparkLocal}
import junit.framework.TestCase
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.{Assert, Test}

class TestFilterFunzionali extends EnvironmentSparkTest { // TestCase with SparkLocal {

  var df: DataFrame = null

  override def setUp(): Unit = {

    super.setUp()

    val rdd = Environment.getSpark.sparkContext.parallelize(List(
      Row("2020-12-01", false, "202012"),
      Row("2020-12-01", true, "202012"),
      Row("2020-12-15", true, "202012"),
      Row("2021-01-01", false, "202101"),
      Row("2021-01-01", true, "202101"),
      Row("2021-02-01", false, "202102"),
      Row("2021-02-01", true, "202102"),
      Row("2021-03-01", true, "202103"),
      Row("2021-04-01", true, "202104")))

    val schema = StructType( Array(
      StructField(FunzionaliSchema.d_data_decorrenza, StringType, nullable = true),
      StructField(FunzionaliSchema.is_nuova_attivazione, BooleanType, nullable = true),
      StructField(FunzionaliSchema.annomese_sw, StringType, nullable = true)
      ))

    df = Environment.getSpark.sqlContext.createDataFrame(rdd, schema).cache()
  }

  @Test
  def testPartitioningColumnsFilter(): Unit = {
    val dfResult1 = FilterFunzionali.partitioningColumnsFilter(df, Seq(LocalDate.parse("2020-12-01")))
    Assert.assertEquals(3, dfResult1.count)
    val dfResult2 = FilterFunzionali.partitioningColumnsFilter(df, Seq(LocalDate.parse("2021-01-01")))
    Assert.assertEquals(2, dfResult2.count)
    val dfResult3 = FilterFunzionali.partitioningColumnsFilter(df, Seq(LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01")))
    Assert.assertEquals(4, dfResult3.count)
  }

  @Test
  def testOrdinaryRunFilter(): Unit = {
    val dfResult1 = FilterFunzionali.ordinaryRunFilter(df, LocalDate.parse("2020-12-01"), LocalDate.parse("2021-01-01"))
    Assert.assertEquals(3, dfResult1.count)
    val dfResult2 = FilterFunzionali.ordinaryRunFilter(df, LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01"))
    Assert.assertEquals(2, dfResult2.count)
  }

  @Test
  def testDateFilter(): Unit = {
    var listaDate = Seq(LocalDate.parse("2020-12-01"))
    var listaDateNA = Seq(LocalDate.parse("2020-12-01"))
    val dfResult1 = FilterFunzionali.dateFilter(df, listaDate, listaDateNA)
    Assert.assertEquals(2, dfResult1.count)

    listaDate = Seq(LocalDate.parse("2020-12-15"))
    listaDateNA = Seq(LocalDate.parse("2020-12-01"))
    val dfResult2 = FilterFunzionali.dateFilter(df, listaDate, listaDateNA)
    Assert.assertEquals(1, dfResult2.count)

    listaDate = Seq(LocalDate.parse("2021-01-01"))
    val dfResult3 = FilterFunzionali.dateFilter(df, listaDate, Seq[LocalDate]())
    Assert.assertEquals(1, dfResult3.count)

    listaDateNA = Seq(LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01"))
    val dfResult4 = FilterFunzionali.dateFilter(df, Seq[LocalDate](), listaDateNA)
    Assert.assertEquals(2, dfResult4.count)

    listaDate = Seq(LocalDate.parse("2020-12-01"), LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01"))
    listaDateNA = Seq(LocalDate.parse("2020-12-01"), LocalDate.parse("2020-12-15"), LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01"))
    val dfResult5 = FilterFunzionali.dateFilter(df, listaDate, listaDateNA)
    Assert.assertEquals(7, dfResult5.count)
  }

}
