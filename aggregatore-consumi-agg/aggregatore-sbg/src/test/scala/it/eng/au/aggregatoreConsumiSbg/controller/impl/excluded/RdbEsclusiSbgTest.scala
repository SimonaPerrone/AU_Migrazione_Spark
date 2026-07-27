package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class RdbEsclusiSbgTest extends EnvironmentSparkTest {
  /*def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 31)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008"))
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008"),
          (0, false, null, "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
        )
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", true, "sm3", "notSNam", "10238291008"),
          (0, false, null, "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", true, "sm3", "notSNam", "10238291008")
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
      DailyConsumptionAggSchema.forcedExclusion,
      DailyConsumptionAggSchema.unitMisPrel,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaRdb)
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val outDF = RdbEsclusiSbg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(3, outDF.count)
    RdbEsclusiSbg.run(dailyConsumptionDF)

    val outFields = RdbEsclusiSbg.csvFields
    Assert.assertFalse(outFields.contains("piva_rdb"))
  }

  def testGetAggregato2(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 28)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008"))
        ++
        List(
          (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
        )
        ++
        (1 to 28)
          .map(i => (6, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008"))
        ++
        List(
          (5, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (6, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (6, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
        )
        ++
        (1 to 28)
          .map(i => (6, true, "N", "202001", "002PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008"))
        ++
        List(
          (6, true, "N", "202001", "002PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (6, true, "N", "202001", "002PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
        )
        ++
        (1 to 28)
          .map(i => (3, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008"))
        ++
        List(
          (2, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (1, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (9, true, "N", "202001", "003PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
        )
        ++
        (1 to 28)
          .map(i => (3, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008"))
        ++
        List(
          (2, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
          , (1, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", true, "sm3", "notSNam", "10238291008")
          , (9, true, "N", "202001", "004PDR", "000DIST", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3", "notSNam", "10238291008")
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
      DailyConsumptionAggSchema.forcedExclusion,
      DailyConsumptionAggSchema.unitMisPrel,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaRdb)

    val outDF = RdbEsclusiSbg.getAggregato(dailyConsumptionDF).cache()

    outDF.show(truncate = false)

    Assert.assertEquals("M2", outDF.filter(col("cod_pdr") === "000PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))
    Assert.assertEquals("T1", outDF.filter(col("cod_pdr") === "001PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))
    Assert.assertTrue(outDF.filter(col("cod_pdr") === "002PDR").collect().toList.isEmpty)
    Assert.assertEquals("T3", outDF.filter(col("cod_pdr") === "003PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))
    Assert.assertEquals("Tf", outDF.filter(col("cod_pdr") === "004PDR").take(1).head.getAs[String](DailyConsumptionAggSchema.causale))

    RdbEsclusiSbg.run(dailyConsumptionDF)

  }*/
}
