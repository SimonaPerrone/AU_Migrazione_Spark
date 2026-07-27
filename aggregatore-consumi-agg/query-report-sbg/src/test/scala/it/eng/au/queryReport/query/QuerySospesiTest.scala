package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, RcugasPdrSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.EnvironmentSparkTest
import it.eng.au.queryReport.query.QuerySospesi.outputSchema
import it.eng.au.queryReport.schema.RcugasSospensioniSchema
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DecimalType, LongType}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.sql.{Date, Timestamp}

class QuerySospesiTest extends EnvironmentSparkTest {
  def testGetQuery(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = List(
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202205", Timestamp.valueOf("2022-05-01 00:00:00"), 1, 10, true, "Y", "rdb1", "it1", null),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202205", Timestamp.valueOf("2022-05-02 00:00:00"), 1, 10, true, "Y", "rdb1", "it1", null),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202205", Timestamp.valueOf("2022-05-03 00:00:00"), 1, 10, true, null, "rdb1", "it1", null),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202205", Timestamp.valueOf("2022-05-04 00:00:00"), 1, 10, true, "Y", "rdb2", "it2", null),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202205", Timestamp.valueOf("2022-05-05 00:00:00"), 1, 10, true, "Y", "rdb2", "it2", null),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202205", Timestamp.valueOf("2022-05-06 00:00:00"), 1, 10, true, "Y", "rdb2", "it2", null),
      ("pdr3", "udd3", "pivaUdb3", "dtg3", "cod_remi3", "id-reg3", "cod-prof3", "tipo-client3", "unit3", "pivaDistr3", "202205", Timestamp.valueOf("2022-05-19 00:00:00"), 1, 10, true, "G", "rdb3", "it3", null),
      ("pdr3", "udd3", "pivaUdb3", "dtg3", "cod_remi3", "id-reg3", "cod-prof3", "tipo-client3", "unit3", "pivaDistr3", "202205", Timestamp.valueOf("2022-05-20 00:00:00"), 1, 10, true, "G", "rdb3", "it3", null),
      ("pdr3", "udd3", "pivaUdb3", "dtg3", "cod_remi3", "id-reg3", "cod-prof3", "tipo-client3", "unit3", "pivaDistr3", "202205", Timestamp.valueOf("2022-05-21 00:00:00"), 1, 10, true, "G", "rdb3", "it3", null)
    ).
      toDF(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.pivaUdd,
        DailyConsumptionAggSchema.pivaUdb,
        DailyConsumptionAggSchema.dtg,
        DailyConsumptionAggSchema.codRemi,
        DailyConsumptionAggSchema.idRegClim,
        DailyConsumptionAggSchema.codProfStd,
        DailyConsumptionAggSchema.tipoCliente,
        DailyConsumptionAggSchema.unitMisPrel,
        DailyConsumptionAggSchema.pivaDistr,
        DailyConsumptionAggSchema.annoMese,
        DailyConsumptionAggSchema.date,
        DailyConsumptionAggSchema.value,
        DailyConsumptionAggSchema.errorCode,
        DailyConsumptionAggSchema.isValid,
        DailyConsumptionAggSchema.treatment,
        DailyConsumptionAggSchema.pivaRdb,
        DailyConsumptionAggSchema.pivaIt,
        DailyConsumptionAggSchema.forcedExclusion
      )
      .withColumn(DailyConsumptionAggSchema.session, lit("SBG"))

    val rcugasPdrDF = List(
      ("pdr1", "nIdPdr1"),
      ("pdr2", "nIdPdr2"),
      ("pdr3", "nIdPdr3")
    ).toDF(
      RcugasPdrSchema.t_codice_pdr,
      RcugasPdrSchema.n_id_pdr
    )

    val rcugasSospensioniDF = List(
      ("nIdPdr1", "2022-05-03 00:00:00.0", "2022-05-23 00:00:00.0", "SM1"),
      ("nIdPdr2", "2022-04-01 00:00:00.0", "2022-04-20 00:00:00.0", null),
      ("nIdPdr3", "2022-05-20 00:00:00.0", null, "SM1")
    ).toDF(
      RcugasSospensioniSchema.n_id_pdr,
      RcugasSospensioniSchema.d_data_inizio_sosp,
      RcugasSospensioniSchema.d_data_revoca_sosp,
      RcugasSospensioniSchema.t_cod_causale_sospensione
    )

    val annoMese = DateTime.parse("202205", DateTimeFormat.forPattern("yyyyMM"))
    val startDate = annoMese.toString("yyyy-MM-dd") //the day is already set to 1
    val endDate = annoMese.plusMonths(1).toString("yyyy-MM-dd")

    val startDateColumn = lit(Date.valueOf(startDate))
    val endDateColumn = lit(Date.valueOf(endDate))

    val aggregatoDf = QuerySospesi.getAggregato(dailyConsumptionDF, rcugasSospensioniDF, rcugasPdrDF, startDateColumn, endDateColumn)
      .withColumn("dailyconsumption_executionid", lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn("executionid", lit(Timestamp.valueOf(Environment.getDateRun)))
      .selectExpr(outputSchema.getValues: _*)

    aggregatoDf.show(10, truncate = false)
  }

  def test2(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        ("15104203624102", java.sql.Date.valueOf("2022-04-01"), 27.0, 7809.0, "19", "C1D1", "4", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0407/05608890488_12883420155_202203_TGL_20220407141500_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0401/05608890488_12883420155_202204_IGMG_20220401120214_1.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-02"), 42.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-03"), 34.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-04"), 32.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-05"), 30.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-06"), 28.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-07"), 26.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-08"), 24.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-09"), 29.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-10"), 25.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-11"), 25.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-12"), 24.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-13"), 21.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-14"), 21.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-15"), 18.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-16"), 17.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-17"), 18.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-18"), 19.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-19"), 17.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-20"), 18.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-21"), 20.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-22"), 20.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-23"), 19.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-24"), 19.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-25"), 20.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-26"), 18.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-27"), 17.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-28"), 16.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-29"), 16.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip"),
        ("15104203624102", java.sql.Date.valueOf("2022-04-30"), 15.0, 7809.0, "19", "C1D1", "1", "0", "05608890488", "12883420155", "11957540153", "10238291008", "N", "34673600", "U", "SM3", "SBG", "G", null, true, "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip", "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12883420155/2022/0505/05608890488_12883420155_202204_TGL_20220505154502_1_M.zip")
      )).toDF(
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.ca,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.idFormula,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.dtg,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.unitMisPrel,
      DailyConsumptionAggSchema.session,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.leftMeasureLocalFile,
      DailyConsumptionAggSchema.rightMeasureLocalFile
    )
      .withColumn(DailyConsumptionAggSchema.pivaRdb, lit("10238291008"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(false))
      .withColumn(DailyConsumptionAggSchema.annoMese, lit("202204"))

    val rcugasPdrDF = List(
      ("15104203624102", "15104203624102"),
      ("151042036241022", "151042036241022")
    ).toDF(
      RcugasPdrSchema.t_codice_pdr,
      RcugasPdrSchema.n_id_pdr
    )

    val rcugasSospensioniDF = List(
      ("15104203624102", "2022-04-03 00:00:00.0", "2022-04-23 00:00:00.0", "SM1"),
      ("151042036241022", "2022-03-01 00:00:00.0", "2022-03-20 00:00:00.0", null)
    ).toDF(
      RcugasSospensioniSchema.n_id_pdr,
      RcugasSospensioniSchema.d_data_inizio_sosp,
      RcugasSospensioniSchema.d_data_revoca_sosp,
      RcugasSospensioniSchema.t_cod_causale_sospensione
    )

    val annoMese = DateTime.parse("202204", DateTimeFormat.forPattern("yyyyMM"))
    val startDate = annoMese.toString("yyyy-MM-dd") //the day is already set to 1
    val endDate = annoMese.plusMonths(1).toString("yyyy-MM-dd")

    val startDateColumn = lit(Date.valueOf(startDate))
    val endDateColumn = lit(Date.valueOf(endDate))

    val aggregatoDf = QuerySospesi.getAggregato(dailyConsumptionDF, rcugasSospensioniDF, rcugasPdrDF, startDateColumn, endDateColumn)
      .withColumn("dailyconsumption_executionid", lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn("executionid", lit(Timestamp.valueOf(Environment.getDateRun).getTime))
      .selectExpr(outputSchema.getValues: _*)

    aggregatoDf.show(10, truncate = false)
  }
}
