package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioPdrG

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioGOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class IdDettaglioGTest extends EnvironmentSparkTest {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(List(
      (java.sql.Date.valueOf("2020-12-12"), 0.4, Some(1.0), 0, false, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
      (java.sql.Date.valueOf("2020-12-12"), 0.5, Some(1.0), 0, true, "N", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
      (java.sql.Date.valueOf("2020-12-12"), 2.0, Some(1.0), 0, true, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
      (java.sql.Date.valueOf("2020-12-12"), 0.1, Some(1.0), 10, true, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false)) ++

      (1 to 31).map(i => (java.sql.Date.valueOf(s"2020-12-$i"), 0.1, Some(1.0), 10, true, "G", "202012", "001PDR", "001RDB", "001DISTR", "001IT", "001UDD", "001UDB", "Y", "001REMI", 0.1, "001IDCL", "CODPROF_0", "U", "T", "sm3", false)) ++

      List(
        (java.sql.Date.valueOf("2020-12-01"), 0.1, Some(1.0), 5, true, "Y", "202012", "002PDR", "002RDB", "002DISTR", "002IT", "002UDD", "002UDB", "Y", "002REMI", 0.1, "002IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (java.sql.Date.valueOf("2020-12-02"), 0.1, Some(1.0), 5, true, "Y", "202012", "002PDR", "002RDB", "002DISTR", "002IT", "002UDD", "002UDB", "Y", "002REMI", 0.1, "002IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (java.sql.Date.valueOf("2020-12-03"), 0.1, None, 5, true, "G", "202012", "002PDR", "002RDB", "002DISTR", "002IT", "002UDD", "002UDB", "Y", "002REMI", 0.1, "002IDCL", "CODPROF_0", "U", "T", "sm3", false),

        (java.sql.Date.valueOf("2020-12-01"), 0.1, Some(1.0), 10, true, "G", "202012", "003PDR", "003RDB", "003DISTR", "003IT", "003UDD", "003UDB", "Y", "003REMI", 0.1, "003IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (java.sql.Date.valueOf("2020-12-02"), 0.1, Some(1.0), 10, true, "Y", "202012", "003PDR", "003RDB", "003DISTR", "003IT", "003UDD", "003UDB", "Y", "003REMI", 0.1, "003IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (java.sql.Date.valueOf("2020-12-03"), 0.1, None, 10, true, "G", "202012", "003PDR", "003RDB", "003DISTR", "003IT", "003UDD", "003UDB", "Y", "003REMI", 0.1, "003IDCL", "CODPROF_0", "U", "T", "sm3", false),

        (java.sql.Date.valueOf("2020-12-01"), 0.1, None, 8, true, "G", "202012", "004PDR", "004RDB", "004DISTR", "004IT", "004UDD", "004UDB", "Y", "004REMI", 0.1, "004IDCL", "CODPROF_0", "U", "T", "sm3", true),
        (java.sql.Date.valueOf("2020-12-02"), 0.1, None, 8, true, "M", "202012", "004PDR", "004RDB", "004DISTR", "004IT", "004UDD", "004UDB", "Y", "004REMI", 0.1, "004IDCL", "CODPROF_0", "U", "T", "sm3", true),
        (java.sql.Date.valueOf("2020-12-03"), 0.1, None, 8, true, "G", "202012", "004PDR", "004RDB", "004DISTR", "004IT", "004UDD", "004UDB", "Y", "004REMI", 0.1, "004IDCL", "CODPROF_0", "U", "T", "sm3", true)
      )).toDF(
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

    val outDF = IdDettaglioG.getAggregato(dailyConsumptionDF)
      .cache

    outDF.show(truncate = false)
    Assert.assertEquals(3, outDF.count)
    Assert.assertEquals(1, outDF.filter(col(DettaglioGOutputSchema.cod_pdr) === "000PDR").count)
    Assert.assertEquals(1, outDF.filter(col(DettaglioGOutputSchema.cod_pdr) === "001PDR").count)
    Assert.assertEquals(0, outDF.filter(col(DettaglioGOutputSchema.cod_pdr) === "002PDR").count)
    Assert.assertEquals(1, outDF.filter(col(DettaglioGOutputSchema.cod_pdr) === "003PDR").count)
    Assert.assertEquals(0, outDF.filter(col(DettaglioGOutputSchema.cod_pdr) === "004PDR").count)
  }
}
