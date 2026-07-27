package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class UddEsclusiSbgTest extends EnvironmentSparkTest {
  /*def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 31)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "0.5", "sm3", false))
        ++
        (1 to 30)
          .map(i => (6, true, "N", "202001", "999PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "0.5", "sm3", false))
        ++
        List(
          (6, true, "N", "202001", "999PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI_NEW", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "0.5", "sm3", false)
        )
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", null, "sm3", false),
          (0, false, null, "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", null, "sm3", false)
        )
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", null, "sm3", true),
          (0, true, "N", "202001", "001PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", null, "sm3", true),
          (0, true, "N", "202001", "001PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", null, "sm3", true),
          (0, false, null, "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", null, "sm3", true)
        )
    ).toDF(
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
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.session,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.unitMisPrel,
      DailyConsumptionAggSchema.forcedExclusion)
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val outDF = UddEsclusiSbg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(5, outDF.count)
    UddEsclusiSbg.run(dailyConsumptionDF)
  }*/
}
