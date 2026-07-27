package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.EnvironmentSparkTest
import org.apache.spark.sql.functions.lit

import java.sql.Timestamp

class QueryAggregatoTest extends EnvironmentSparkTest {
  def testGetQuery(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = List(
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb1", "it1", null, "SBG"),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb1", "it1", null, "SBG"),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-15 00:00:00"), 1, 1.0, 10, true, null, "rdb1", "it1", null, "SBG"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202104", Timestamp.valueOf("2021-04-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb2", "it2", null, "SBG"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb2", "it2", null, "SBG"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb2", "it2", null, "SBG"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, null, "rdb2", "it2", null, "SBG"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "G", "rdb2", "it2", null, "SBG"),
      ("pdr2", "udd2", null, "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "G", "rdb2", "it2", null, "SBG")
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
        DailyConsumptionAggSchema.valuef3,
        DailyConsumptionAggSchema.errorCode,
        DailyConsumptionAggSchema.isValid,
        DailyConsumptionAggSchema.treatment,
        DailyConsumptionAggSchema.pivaRdb,
        DailyConsumptionAggSchema.pivaIt,
        DailyConsumptionAggSchema.forcedExclusion,
        DailyConsumptionAggSchema.session
      )
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val aggregatoDf = QueryAggregato.getQueryDF(dailyConsumptionDF)
    aggregatoDf.show(10, truncate = false)
  }
}