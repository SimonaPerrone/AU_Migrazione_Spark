package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioPdrG

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class UdbDettaglioGTest extends EnvironmentSparkTest {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (date, 0.4, 0.5, 0, false, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (date, 0.5, 0.5, 0, true, "N", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (date, 2.0, 0.5, 0, true, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (date, 0.1, 0.5, 10, true, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false)
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
      DailyConsumptionAggSchema.pivaRdb,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.dtg,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.ca,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.unitMisPrel,
      DailyConsumptionAggSchema.forcedExclusion
    )
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.session, lit("AGG_S1_PRE"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val outDF = UdbDettaglioG.getAggregato(dailyConsumptionDF)
      .cache

    outDF.show(truncate = false)

    Assert.assertEquals(1, outDF.count)
    Assert.assertEquals(1, outDF.filter(col("PRELIEVO_GIORN_12") === lit(2)).count)
  }
}
