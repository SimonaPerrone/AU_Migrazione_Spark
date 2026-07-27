package it.au.misure.ee_switching.filterPod

import java.time.LocalDate
import it.au.misure.ee_switching.model.schema.hive.StoriciSchema
import it.au.misure.ee_switching.utility.environment.Environment
import it.au.misure.ee_switching.utility.EnvironmentSparkTest
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.{Assert, Test}

class TestFilterStorici extends EnvironmentSparkTest {

  var df: DataFrame = null

  override def setUp(): Unit = {

    super.setUp()

    val rdd = Environment.getSpark.sparkContext.parallelize(List(
      Row("2020-12-01", 2020, 10, "O", "202012"),
      Row("2020-12-01", 2020, 11, "O", "202012"),
      Row("2020-12-15", 2020, 11, "O", "202012"),
      Row("2021-01-01", 2020, 11, "O", "202101"),
      Row("2021-01-01", 2020, 12, "O", "202101"),
      Row("2021-02-01", 2020, 11, "O", "202102"),
      Row("2021-02-01", 2021, 1, "O", "202102"),
      Row("2021-02-01", 2019, 11, "O", "202102"),
      Row("2021-03-01", 2020, 11, "O", "202103"),
      Row("2021-04-01", 2020, 11, "O", "202104"),
      Row("2020-12-01", 2020, 10, "F", "202012"),
      Row("2020-12-01", 2020, 11, "F", "202012"),
      Row("2020-12-15", 2020, 11, "F", "202012"),
      Row("2020-12-15", 2020, 12, "F", "202012"),
      Row("2021-01-01", 2020, 11, "F", "202101"),
      Row("2021-01-01", 2020, 12, "F", "202101"),
      Row("2021-02-01", 2020, 11, "F", "202102"),
      Row("2021-02-01", 2020, 11, "F", "202102"),
      Row("2021-03-01", 2020, 11, "F", "202103"),
      Row("2021-04-01", 2020, 11, "F", "202104")))

    val schema = StructType( Array(
      StructField(StoriciSchema.d_data_decorrenza, StringType, nullable = true),
      StructField(StoriciSchema.anno_misura, IntegerType, nullable = true),
      StructField(StoriciSchema.mese_misura, IntegerType, nullable = true),
      StructField(StoriciSchema.trattamento_online, StringType, nullable = true),
      StructField(StoriciSchema.annomese_sw, StringType, nullable = true)
      ))

    df = Environment.getSpark.sqlContext.createDataFrame(rdd, schema).cache()
  }

  def assertThrows[E](f: => Unit)(implicit eType:scala.reflect.ClassTag[E]): Unit = {
    try {
      f
    } catch {
      case e: Exception =>
        if ( eType.runtimeClass.isAssignableFrom(e.getClass))
          return;
    }
    throw new AssertionError("Expected error of type " + eType.runtimeClass.getName )
  }

  @Test
  def testPartitioningColumnsFilter(): Unit = {
    val dfResult1 = FilterStorici.partitioningColumnsFilter(df, Seq(LocalDate.parse("2020-12-01")))
    Assert.assertEquals(7, dfResult1.count)
    val dfResult2 = FilterStorici.partitioningColumnsFilter(df, Seq(LocalDate.parse("2021-01-01")))
    Assert.assertEquals(4, dfResult2.count)
    val dfResult3 = FilterStorici.partitioningColumnsFilter(df, Seq(LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01")))
    Assert.assertEquals(9, dfResult3.count)
  }

  @Test
  def testOrdinaryRunFilter(): Unit = {
    val dfResult1 = FilterStorici.ordinaryRunFilter(df, LocalDate.parse("2020-12-01"), LocalDate.parse("2021-01-01"))
    Assert.assertEquals(6, dfResult1.count)
    val dfResult2 = FilterStorici.ordinaryRunFilter(df, LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01"))
    Assert.assertEquals(4, dfResult2.count)
  }

  @Test
  def testDateFilter(): Unit = {
    val emptySeq = Seq[LocalDate]()

    var listaDate = Seq(LocalDate.parse("2020-12-01"))
    val dfResult1 = FilterStorici.dateFilter(df, listaDate, emptySeq)
    Assert.assertEquals(2, dfResult1.count)

    listaDate = Seq(LocalDate.parse("2020-12-15"))
    val dfResult2 = FilterStorici.dateFilter(df, listaDate, emptySeq)
    Assert.assertEquals(0, dfResult2.count)

    listaDate = Seq(LocalDate.parse("2021-01-01"))
    val dfResult3 = FilterStorici.dateFilter(df, listaDate, Seq[LocalDate]())
    Assert.assertEquals(2, dfResult3.count)

    val dfResult4 = FilterStorici.dateFilter(df, Seq(LocalDate.parse("2019-12-01")), emptySeq)
    Assert.assertEquals(0, dfResult4.count)

    listaDate = Seq(LocalDate.parse("2020-12-01"), LocalDate.parse("2021-01-01"))
    val dfResult5 = FilterStorici.dateFilter(df, listaDate, emptySeq)
    Assert.assertEquals(6, dfResult5.count)

    listaDate = Seq(LocalDate.parse("2020-12-01"), LocalDate.parse("2021-01-01"), LocalDate.parse("2021-02-01"))
    assertThrows[IllegalArgumentException] { FilterStorici.dateFilter(df, listaDate, emptySeq) }

    assertThrows[IllegalArgumentException] { FilterStorici.dateFilter(df, emptySeq, emptySeq) }
  }

}
