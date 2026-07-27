package it.au.misure.ee_switching.filterPod

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import it.au.misure.ee_switching.model.schema.hive.FunzionaliSchema
import it.au.misure.ee_switching.utility.environment.Environment
import it.au.misure.ee_switching.utility.EnvironmentSparkTest
import junit.framework.TestCase
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.{Assert, Test}

class TestFilterPod extends EnvironmentSparkTest {

  var df: DataFrame = null

  override def setUp(): Unit = {

    super.setUp()

    val rdd = Environment.getSpark.sparkContext.parallelize(List(
      Row("10000000000", "01000000001", "00000000000000", Timestamp.valueOf(LocalDateTime.parse("2020-01-01 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))),
      Row("10000000001", "01000000001", "00000000000001", Timestamp.valueOf(LocalDateTime.parse("2020-01-01 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))),
      Row("10000000002", "01000000002", "00000000000002", Timestamp.valueOf(LocalDateTime.parse("2020-01-01 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))),
      Row("10000000003", "01000000002", "00000000000003", Timestamp.valueOf(LocalDateTime.parse("2020-01-02 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))),
      Row("10000000004", "01000000003", "00000000000004", Timestamp.valueOf(LocalDateTime.parse("2020-01-02 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))),
      Row("10000000004", "01000000003", "00000000000005", Timestamp.valueOf(LocalDateTime.parse("2020-01-02 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))),
      Row("10000000004", "01000000004", "00000000000006", Timestamp.valueOf(LocalDateTime.parse("2020-01-03 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))))

    val schema = StructType( Array(
      StructField(FunzionaliSchema.piva_distr, StringType, nullable = true),
      StructField(FunzionaliSchema.piva_udd, StringType, nullable = true),
      StructField(FunzionaliSchema.pod14, StringType, nullable = true),
      StructField(FunzionaliSchema.d_caricamento, TimestampType, nullable = true)
      ))

    df = Environment.getSpark.sqlContext.createDataFrame(rdd, schema).cache
  }

  @Test
  def testPodFilter(): Unit = {
    val dfResult1 = FilterFunzionali.podFilter(df, Seq("00000000000000"))
    Assert.assertEquals(1, dfResult1.count)
    val dfResult2 = FilterFunzionali.podFilter(df, Seq("00000000000000","00000000000001","00000000000002"))
    Assert.assertEquals(3, dfResult2.count)
    val dfResult3 = FilterFunzionali.podFilter(df, Seq("0000000"))
    Assert.assertEquals(0, dfResult3.count)
  }

  @Test
  def testDistributoreFilter(): Unit = {
    val dfResult1 = FilterFunzionali.distributoreFilter(df, Seq("10000000000"))
    Assert.assertEquals(1, dfResult1.count)
    val dfResult2 = FilterFunzionali.distributoreFilter(df, Seq("10000000000","10000000001","10000000002"))
    Assert.assertEquals(3, dfResult2.count)
    val dfResult3 = FilterFunzionali.distributoreFilter(df, Seq("10000000003", "10000000004"))
    Assert.assertEquals(4, dfResult3.count)
    val dfResult4 = FilterFunzionali.distributoreFilter(df, Seq("0000000"))
    Assert.assertEquals(0, dfResult4.count)
  }

  @Test
  def testUddFilter(): Unit = {
    val dfResult1 = FilterFunzionali.uddFilter(df, Seq("01000000001"))
    Assert.assertEquals(2, dfResult1.count)
    val dfResult2 = FilterFunzionali.uddFilter(df, Seq("01000000001","01000000002"))
    Assert.assertEquals(4, dfResult2.count)
    val dfResult3 = FilterFunzionali.uddFilter(df, Seq("01000000001", "01000000003"))
    Assert.assertEquals(4, dfResult3.count)
    val dfResult4 = FilterFunzionali.uddFilter(df, Seq("0000000"))
    Assert.assertEquals(0, dfResult4.count)
  }

  @Test
  def testTimestampFilter(): Unit = {
    val dfResult = FilterFunzionali.timestampFilter(df, Timestamp.valueOf(LocalDateTime.parse("2020-01-01 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))
    Assert.assertEquals(3, dfResult.count)
  }

  @Test
  def testCoppieDistrUddFilter(): Unit = {
    val dfResult = FilterFunzionali.coppieDistrUddFilter(df, Seq(("10000000001","01000000001"), ("10000000002","01000000002")))
    Assert.assertEquals(2, dfResult.count)
  }

}
