package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, EsclusiOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class RdbEsclusiTest extends EnvironmentSparkTest {
  /*def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 31)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false))
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false),
          (0, false, null, "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
        )
    ).toDF(
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.dtg,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.ca,
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.session,
      DailyConsumptionAggSchema.forcedExclusion
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.pivaIt, lit("notSNam"))
      .withColumn(DailyConsumptionAggSchema.pivaRdb, lit("10238291008"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val outDF = RdbEsclusi.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(1, outDF.count)
    RdbEsclusi.run(dailyConsumptionDF)

    val outFields = RdbEsclusi.csvFields
    Assert.assertFalse(outFields.contains("piva_rdb"))
    Assert.assertEquals(31, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "000PDR").take(1).head.getAs[Int](EsclusiOutputSchema.prelievo_aggregato))

  }

  def testGetAggregato2(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 28)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false))
        ++
        List(
          (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
        )
        ++
        (1 to 28)
          .map(i => (6, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false))
        ++
        List(
          (5, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (6, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (6, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
        )
        ++
        (1 to 28)
          .map(i => (6, true, "N", "202001", "002PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false))
        ++
        List(
          (6, true, "N", "202001", "002PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (6, true, "N", "202001", "002PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
        )
        ++
        (1 to 28)
          .map(i => (3, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false))
        ++
        List(
          (2, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (1, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (9, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
        )
        ++
        (1 to 28)
          .map(i => (3, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false))
        ++
        List(
          (2, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
          , (1, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", true)
          , (9, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false)
        )
    ).toDF(
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.dtg,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.ca,
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.session,
      DailyConsumptionAggSchema.forcedExclusion
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.pivaIt, lit("notSNam"))
      .withColumn(DailyConsumptionAggSchema.pivaRdb, lit("10238291008"))

    val outDF = RdbEsclusi.getAggregato(dailyConsumptionDF).cache()

    outDF.show(truncate = false)

    Assert.assertEquals("M2", outDF.filter(col("cod_pdr") === "000PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))
    Assert.assertEquals("T1", outDF.filter(col("cod_pdr") === "001PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))
    Assert.assertTrue(outDF.filter(col("cod_pdr") === "002PDR").collect().toList.isEmpty)
    Assert.assertEquals("T3", outDF.filter(col("cod_pdr") === "003PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))
    Assert.assertEquals("Tf", outDF.filter(col("cod_pdr") === "004PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))
    Assert.assertEquals(31, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "000PDR").take(1).head.getAs[Int](EsclusiOutputSchema.prelievo_aggregato))
    Assert.assertEquals(31, outDF.filter(col(EsclusiOutputSchema.cod_pdr) === "004PDR").take(1).head.getAs[Int](EsclusiOutputSchema.prelievo_aggregato))

    RdbEsclusi.run(dailyConsumptionDF)

  }*/
}
