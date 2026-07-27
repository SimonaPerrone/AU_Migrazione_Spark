package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerenti

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class UddIncoerentiSbgTest extends EnvironmentSparkTest {
//  def testMergeAndZip(): Unit = {
//    UddIncoerenti.mergeAndZipFiles(List("f1", "f2", "f3")).count()
//  }

  def testGetAggregato(): Unit = {
    Environment.setProperty("pdr.anomali.listaC.enabled", "true")
    Environment.setProperty("pdr.anomali.listaD.enabled", "true")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2022-07-12"), 100000.1, 0, true, "G", "202207", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-13"), 0.5, 0, true, "N", "202207", "001PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-14"), 2.0, 0, true, "G", "202207", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-15"), 25000.0, 10, true, "G", "202207", "003PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-16"), 75000.1, 10, true, "G", "202207", "003PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG")
      )
    ).toDF(
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.pdr,
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
      DailyConsumptionAggSchema.session
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1000.0)) //in questo modo i PdR non possono essere incoerenti GDM
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))

    val outDF = UddIncoerentiSbg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(1, outDF.filter(col("prelievo_giorn_12") === lit(100000)).count)

    UddIncoerentiSbg.run(dailyConsumptionDF)

    val outFields = UddIncoerentiSbg.csvFields
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
      , "prelievo_giorn_1"
      , "prelievo_giorn_2"
      , "prelievo_giorn_3"
      , "prelievo_giorn_4"
      , "prelievo_giorn_5"
      , "prelievo_giorn_6"
      , "prelievo_giorn_7"
      , "prelievo_giorn_8"
      , "prelievo_giorn_9"
      , "prelievo_giorn_10"
      , "prelievo_giorn_11"
      , "prelievo_giorn_12"
      , "prelievo_giorn_13"
      , "prelievo_giorn_14"
      , "prelievo_giorn_15"
      , "prelievo_giorn_16"
      , "prelievo_giorn_17"
      , "prelievo_giorn_18"
      , "prelievo_giorn_19"
      , "prelievo_giorn_20"
      , "prelievo_giorn_21"
      , "prelievo_giorn_22"
      , "prelievo_giorn_23"
      , "prelievo_giorn_24"
      , "prelievo_giorn_25"
      , "prelievo_giorn_26"
      , "prelievo_giorn_27"
      , "prelievo_giorn_28"
      , "prelievo_giorn_29"
      , "prelievo_giorn_30"
      , "prelievo_giorn_31")

    Assert.assertEquals(outFields.size, outFields.toSet.size)
    outFields.foreach(f => {
      println(f)
      Assert.assertTrue(outFieldsSet.contains(f))
    })
    Assert.assertEquals(outFieldsSet, outFields.toSet)
  }

  def testGetAggregatoEmptyJoin(): Unit = {
    Environment.setProperty("pdr.anomali.listaC.enabled", "true")
    Environment.setProperty("pdr.anomali.listaD.enabled", "true")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2022-07-12"), 100000.1, 0, true, "G", "202207", "100PDR", "000DISTR", "000IT", "001UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-13"), 0.5, 0, true, "N", "202207", "101PDR", "000DISTR", "000IT", "001UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-14"), 2.0, 0, true, "G", "202207", "102PDR", "000DISTR", "000IT", "001UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-15"), 25000.0, 10, true, "G", "202207", "103PDR", "000DISTR", "000IT", "001UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-16"), 75000.1, 10, true, "G", "202207", "103PDR", "000DISTR", "000IT", "001UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG")
      )
    ).toDF(
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.pdr,
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
      DailyConsumptionAggSchema.session
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1000.0)) //in questo modo i PdR non possono essere incoerenti GDM
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))

    val outDF = UddIncoerentiSbg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(1, outDF.filter(col("prelievo_giorn_12") === lit(100000)).count)

    UddIncoerentiSbg.run(dailyConsumptionDF)
  }

  def testGetAggregatoCONFonly(): Unit = {
    Environment.setProperty("pdr.anomali.listaC.enabled", "true")
    Environment.setProperty("pdr.anomali.listaD.enabled", "true")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2022-07-12"), 100000.1, 0, true, "G", "202207", "200PDR", "000DISTR", "000IT", "002UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-13"), 0.5, 0, true, "N", "202207", "201PDR", "000DISTR", "000IT", "002UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-14"), 2.0, 0, true, "G", "202207", "202PDR", "000DISTR", "000IT", "002UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-15"), 25000.0, 10, true, "G", "202207", "203PDR", "000DISTR", "000IT", "002UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG"),
        (java.sql.Date.valueOf("2022-07-16"), 75000.1, 10, true, "G", "202207", "203PDR", "000DISTR", "000IT", "002UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "SBG")
      )
    ).toDF(
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.pdr,
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
      DailyConsumptionAggSchema.session
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1000.0)) //in questo modo i PdR non possono essere incoerenti GDM
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))

    val outDF = UddIncoerentiSbg.getAggregato(dailyConsumptionDF)

    outDF.show(truncate = false)

    Assert.assertEquals(1, outDF.filter(col("prelievo_giorn_12") === lit(100000)).count)

    UddIncoerentiSbg.run(dailyConsumptionDF)
  }

}
