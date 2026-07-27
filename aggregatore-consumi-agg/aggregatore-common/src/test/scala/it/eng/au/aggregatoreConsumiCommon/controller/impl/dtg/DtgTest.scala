package it.eng.au.aggregatoreConsumiCommon.controller.impl.dtg

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DTGOutputSchema, DailyConsumptionAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class DtgTest extends EnvironmentSparkTest {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2020-12-12"), 0.4, Some(1.0), 0, false, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "10238291008", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (java.sql.Date.valueOf("2020-12-12"), 0.5, Some(1.0), 0, true, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (java.sql.Date.valueOf("2020-12-12"), 2.0, Some(1.0), 0, true, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "10238291008", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false),
        (java.sql.Date.valueOf("2020-12-12"), 0.1, Some(1.0), 10, true, "G", "202012", "000PDR", "000RDB", "000DISTR", "000IT", "000UDD", "10238291008", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "sm3", false)) ++

        (1 to 31).map(i => (java.sql.Date.valueOf(s"2020-12-$i"), 0.1, Some(1.0), 10, true, "G", "202012", "001PDR", "001RDB", "001DISTR", "001IT", "001UDD", "10238291008", "Y", "001REMI", 0.1, "001IDCL", "CODPROF_0", "U", "T", "sm3", false)) ++

        List(
          (java.sql.Date.valueOf("2020-12-01"), 0.1, Some(1.0), 5, true, "G", "202012", "002PDR", "002RDB", "002DISTR", "002IT", "002UDD", "002UDB", "Y", "002REMI", 0.1, "002IDCL", "CODPROF_0", "U", "T", "sm3", false),
          (java.sql.Date.valueOf("2020-12-02"), 0.1, Some(1.0), 5, true, "G", "202012", "002PDR", "002RDB", "002DISTR", "002IT", "002UDD", "002UDB", "Y", "002REMI", 0.1, "002IDCL", "CODPROF_0", "U", "T", "sm3", false),
          (java.sql.Date.valueOf("2020-12-03"), 0.1, None, 5, true, "G", "202012", "002PDR", "002RDB", "002DISTR", "002IT", "002UDD", "10238291008", "Y", "002REMI", 0.1, "002IDCL", "CODPROF_0", "U", "T", "sm3", false),

          (java.sql.Date.valueOf("2020-12-01"), 0.1, Some(1.0), 10, true, "G", "202012", "003PDR", "003RDB", "003DISTR", "003IT", "003UDD", "10238291008", "Y", "003REMI", 0.1, "003IDCL", "CODPROF_0", "U", "T", "sm3", false),
          (java.sql.Date.valueOf("2020-12-02"), 0.1, Some(1.0), 10, true, "G", "202012", "003PDR", "003RDB", "003DISTR", "003IT", "003UDD", "003UDB", "Y", "003REMI", 0.1, "003IDCL", "CODPROF_0", "U", "T", "sm3", false),
          (java.sql.Date.valueOf("2020-12-03"), 0.1, None, 10, true, "G", "202012", "003PDR", "003RDB", "003DISTR", "003IT", "003UDD", "10238291008", "Y", "003REMI", 0.1, "003IDCL", "CODPROF_0", "U", "T", "sm3", false),

          (java.sql.Date.valueOf("2020-12-01"), 0.1, None, 8, true, "G", "202012", "004PDR", "004RDB", "004DISTR", "004IT", "004UDD", "10238291008", "Y", "004REMI", 0.1, "004IDCL", "CODPROF_0", "U", "T", "sm3", true),
          (java.sql.Date.valueOf("2020-12-02"), 0.1, None, 8, true, "G", "202012", "004PDR", "004RDB", "004DISTR", "004IT", "004UDD", "004UDB", "Y", "004REMI", 0.1, "004IDCL", "CODPROF_0", "U", "T", "sm3", true),
          (java.sql.Date.valueOf("2020-12-03"), 0.1, None, 8, true, "G", "202012", "004PDR", "004RDB", "004DISTR", "004IT", "004UDD", "10238291008", "Y", "004REMI", 0.1, "004IDCL", "CODPROF_0", "U", "T", "sm3", true))
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
      .withColumn(DailyConsumptionAggSchema.session, lit("AGG_S1_PRE"))
      .withColumn(DailyConsumptionAggSchema.tCodIstat, lit("cod1"))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val outDF = Dtg.getAggregato(dailyConsumptionDF)
      .cache

    outDF.show(truncate = false)
    Assert.assertEquals(3, outDF.count)
    Assert.assertEquals(1, outDF.filter(col(DTGOutputSchema.cod_pdr) === "000PDR").count)
    Assert.assertEquals(1, outDF.filter(col(DTGOutputSchema.cod_pdr) === "001PDR").count)
    Assert.assertEquals(0, outDF.filter(col(DTGOutputSchema.cod_pdr) === "002PDR").count)
    Assert.assertEquals(1, outDF.filter(col(DTGOutputSchema.cod_pdr) === "003PDR").count)
    Assert.assertEquals(0, outDF.filter(col(DTGOutputSchema.cod_pdr) === "004PDR").count)
  }

  def testDtg(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (date, 0.4, 1.0, 0, false, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.5, 1.0, 0, false, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.2, 1.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1, 1.0, 10, true, "G", "202012", "0a0PDR", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1, 1.0, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1")
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
      .withColumn(DailyConsumptionAggSchema.pivaUdb, lit("10238291008"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.tCodIstat, lit("COD"))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val outDF = Dtg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(2, outDF.count)
    Assert.assertEquals(1, outDF.filter(col("PRELIEVO_GIORN_12") === lit(0.3) and col("cod_pdr") === lit("000PDR")).count)

    val outFields = Dtg.csvFields
    val outFieldsSet = Set("data"
      , "cod_pdr"
      , "piva_distr"
      , "piva_it"
      , "piva_udd"
      , "piva_udb"
      , "dtg"
      , "cod_remi"
      , "prel_annuo_prev"
      , "id_reg_clim"
      , "cod_prof_prel_std"
      , "trattamento"
      , "tipo_cliente"
      , "un_mis_prel"
      , "ISTAT"
      , "PRELIEVO_GIORN_1"
      , "PRELIEVO_GIORN_2"
      , "PRELIEVO_GIORN_3"
      , "PRELIEVO_GIORN_4"
      , "PRELIEVO_GIORN_5"
      , "PRELIEVO_GIORN_6"
      , "PRELIEVO_GIORN_7"
      , "PRELIEVO_GIORN_8"
      , "PRELIEVO_GIORN_9"
      , "PRELIEVO_GIORN_10"
      , "PRELIEVO_GIORN_11"
      , "PRELIEVO_GIORN_12"
      , "PRELIEVO_GIORN_13"
      , "PRELIEVO_GIORN_14"
      , "PRELIEVO_GIORN_15"
      , "PRELIEVO_GIORN_16"
      , "PRELIEVO_GIORN_17"
      , "PRELIEVO_GIORN_18"
      , "PRELIEVO_GIORN_19"
      , "PRELIEVO_GIORN_20"
      , "PRELIEVO_GIORN_21"
      , "PRELIEVO_GIORN_22"
      , "PRELIEVO_GIORN_23"
      , "PRELIEVO_GIORN_24"
      , "PRELIEVO_GIORN_25"
      , "PRELIEVO_GIORN_26"
      , "PRELIEVO_GIORN_27"
      , "PRELIEVO_GIORN_28"
      , "PRELIEVO_GIORN_29"
      , "PRELIEVO_GIORN_30"
      , "PRELIEVO_GIORN_31")

    Assert.assertEquals(outFields.size, outFields.toSet.size)
    outFields.foreach(f => {
      println(f);
      Assert.assertTrue(outFieldsSet.contains(f))
    })
    Assert.assertEquals(outFieldsSet, outFields.toSet)
  }
}
