package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioPdrG

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class UddDettaglioGTest extends EnvironmentSparkTest {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (date, 0.4, 1.0, 0, false, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.5, 1.0, 0, true, "N", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 2.0, 1.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1, 1.0, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1")
      )
    ).toDF(
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.pivaRdb,
      DailyConsumptionAggSchema.dtg,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.ca,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.session
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val outDF = UddDettaglioGSbg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(1, outDF.count)
    Assert.assertEquals(1, outDF.filter(col("PRELIEVO_GIORN_12") === lit(2)).count)

    UddDettaglioGSbg.run(dailyConsumptionDF)
  }
}
