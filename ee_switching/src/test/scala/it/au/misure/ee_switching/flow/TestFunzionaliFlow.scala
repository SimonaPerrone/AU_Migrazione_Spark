package it.au.misure.ee_switching.flow

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import it.au.misure.ee_switching.model.schema.hive.{FunzionaliCompressedSchema, FunzionaliSchema}
import it.au.misure.ee_switching.utility.Constants.{FILENAME_TIMESTAMP_PATTERN, XML_CHUNK_NAME_FIELD}
import it.au.misure.ee_switching.utility.environment.Environment
import it.au.misure.ee_switching.utility.{EnvironmentSparkTest, PropertyUtility}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.junit.{Assert, Test}

class TestFunzionaliFlow extends EnvironmentSparkTest {

  var df: DataFrame = null
  var dfLead: DataFrame = null

  override def setUp(): Unit = {

    super.setUp()

    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext

    val rdd = sc.parallelize(List(
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000001", "00000000000000", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-11", "F2G", "202012", "10000000000", "01000000001", "00000000000000", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000001", "00000000000001", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000001", "00000000000002", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000001", "000000000000015", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000001", "000000000000016", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000001", "000000000000017", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000001", "000000000000018", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000002", "00000000000004", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000002", "00000000000005", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000000", "01000000002", "00000000000006", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000001", "01000000002", "00000000000007", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000001", "01000000002", "00000000000008", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000001", "01000000002", "00000000000009", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000001", "01000000003", "00000000000010", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000001", "01000000003", "00000000000011", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "F2G", "202012", "10000000001", "01000000003", "00000000000012", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "SNF", "202012", "10000000001", "01000000003", "00000000000013", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "SNF", "202012", "10000000001", "01000000003", "00000000000019", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "SNF", "202012", "10000000001", "01000000003", "00000000000020", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
      Row("2021-03-10", "SNF", "202012", "10000000001", "01000000003", "00000000000021", "DP9876", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
    ))

    var schema = StructType(Array(
      StructField(FunzionaliSchema.d_data_decorrenza, StringType, nullable = true),
      StructField(FunzionaliSchema.nome_flusso, StringType, nullable = true),
      StructField(FunzionaliSchema.annomese_sw, StringType, nullable = true),
      StructField(FunzionaliSchema.piva_distr, StringType, nullable = true),
      StructField(FunzionaliSchema.piva_udd, StringType, nullable = true),
      StructField(FunzionaliSchema.pod14, StringType, nullable = true),
      StructField(FunzionaliSchema.t_cod_contr_disp, StringType, nullable = true)
    ))

    for(field <- FunzionaliCompressedSchema.getValues)
      if(!field.equals(FunzionaliSchema.pod14.toString) && !field.equals(FunzionaliSchema.nome_flusso.toString) && !field.equals(FunzionaliSchema.d_data_decorrenza.toString))
        schema = schema.add(StructField(field, StringType, nullable = true))

    df = sqlContext.createDataFrame(rdd, schema).cache


    val rddLead = sc.parallelize(List(
      Row(Timestamp.valueOf(LocalDateTime.now), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.now), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.now), "2020-12-02", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.now), "2020-12-03", "202012", "00000000000001"),
      Row(Timestamp.valueOf(LocalDateTime.now), "2020-12-03", "202012", "00000000000002"),
      Row(Timestamp.valueOf(LocalDateTime.now), "2020-12-03", "202012", "00000000000002"),
      Row(Timestamp.valueOf(LocalDateTime.now), "2020-12-03", "202012", "00000000000002"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-01 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-01 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-02 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-02 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-02 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-02 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-02", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-03 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-01", "202012", "00000000000000"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-03 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-03", "202012", "00000000000001"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-03 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-03", "202012", "00000000000002"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-03 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-03", "202012", "00000000000002"),
      Row(Timestamp.valueOf(LocalDateTime.parse("2020-01-04 15:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), "2020-12-03", "202012", "00000000000002")
    ))

    val schemaLead = StructType( Array(
      StructField(FunzionaliSchema.d_caricamento, TimestampType, nullable = true),
      StructField(FunzionaliSchema.d_data_decorrenza, StringType, nullable = true),
      StructField(FunzionaliSchema.annomese_sw, StringType, nullable = true),
      StructField(FunzionaliSchema.pod14, StringType, nullable = true)
    ))

    dfLead = sqlContext.createDataFrame(rddLead, schemaLead).cache

  }

  @Test
  def testGetLastDataVersion(): Unit = {
//    FunzionaliFlow.getLastDataVersion(dfLead).show()
    Assert.assertEquals(4, FunzionaliFlow.getLastDataVersion(dfLead).count)
    Assert.assertEquals(4, FunzionaliFlow.getLastDataVersion(dfLead).filter(to_date(col(FunzionaliSchema.d_caricamento)) === LocalDate.now.toString).count)
  }

  @Test
  def testAssignPodToXmlChunk(): Unit = {
    val timestampRun = LocalDateTime.now(ZoneId.of(PropertyUtility.getTimeZone)).format(DateTimeFormatter.ofPattern(FILENAME_TIMESTAMP_PATTERN))
//    FunzionaliFlow.assignPodToXmlChunk(df).show(false)
    Assert.assertEquals(12, FunzionaliFlow.assignPodToXmlChunk(df, timestampRun).select(XML_CHUNK_NAME_FIELD).distinct().count())
  }

}
