package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, EsclusiOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class IdEsclusiTest extends EnvironmentSparkTest {
  /*def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 31)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", false))
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", false),
          (0, false, null, "202001", "000PDR", "000DIST", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", false),
          (0, true, "N", "202001", "002PDR", "002DIST", "002IT", "002UDD", "002UDB", "002RDB", "Y", "002REMI", 0.1, 1.0, "002IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", true)
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

    val outDF = IdEsclusi.getAggregato(dailyConsumptionDF)
    outDF.cache

    outDF.show(truncate = false)

    Assert.assertEquals(2, outDF.count)
    Assert.assertEquals(1, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "000PDR").count)
    Assert.assertEquals(0, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "001PDR").count)
    Assert.assertEquals(1, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "002PDR").count)
    Assert.assertEquals(31, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "000PDR").take(1).head.getAs[Int](EsclusiOutputSchema.prelievo_aggregato))
    Assert.assertEquals(1, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "002PDR").take(1).head.getAs[Int](EsclusiOutputSchema.prelievo_aggregato))
  }*/
}
