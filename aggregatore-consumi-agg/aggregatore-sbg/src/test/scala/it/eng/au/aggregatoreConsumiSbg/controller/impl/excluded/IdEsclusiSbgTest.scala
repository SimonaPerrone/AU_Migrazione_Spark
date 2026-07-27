package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class IdEsclusiSbgTest extends EnvironmentSparkTest {

  /*def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 31)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", false))
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", false),
          (0, false, null, "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", false)
        )
        ++ List(
        (0, true, "G", "202001", "001PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", true),
        (0, true, "Y", "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", true)
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
      DailyConsumptionAggSchema.unitMisPrel,
      DailyConsumptionAggSchema.forcedExclusion
    )
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))


    val outDF = IdEsclusiSbg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(3, outDF.count)
  }*/
}
