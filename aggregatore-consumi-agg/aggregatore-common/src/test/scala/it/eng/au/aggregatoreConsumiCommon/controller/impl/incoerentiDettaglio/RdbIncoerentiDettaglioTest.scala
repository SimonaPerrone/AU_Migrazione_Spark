package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.RdbPdrIncoerentiDettaglio
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class RdbIncoerentiDettaglioTest extends EnvironmentSparkTest {

  def testGetAggregato(): Unit = {
    Environment.setProperty("incoerenzaGDM.numberOfDays.threshold", "5")
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2020-12-12"), 1.0, 1.5, 100000.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (java.sql.Date.valueOf("2020-12-13"), 1.0, 1.5, 0.5, 0, true, "N", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (java.sql.Date.valueOf("2020-12-14"), 1.0, 1.5, 2.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (java.sql.Date.valueOf("2020-12-15"), 1.0, 1.5, 0.1, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1")
      )
    ).toDF(
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.coefficient,
      DailyConsumptionAggSchema.valuef3,
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
      .withColumn(DailyConsumptionAggSchema.pivaRdb, lit("0000PIVARDB"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G1,6"))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))
      .withColumn("valueNotSterilizedI", lit(0.0))


    val anomalousPdrs = RdbIncoerentiDettaglio.getAnomalousPdrs(dailyConsumptionDF)
    val outDF = RdbPdrIncoerentiDettaglio.getAggregatoFromPdrs(anomalousPdrs)

    outDF.show(truncate = false)

    //Assert.assertEquals(1, outDF.filter(col("prelievo_non_sterilizzato_giorn_12") === lit(100000.0)).count)

    val outFields = RdbPdrIncoerentiDettaglio.csvFields
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
      , "classe_gruppo_mis"
      , "prelievo_aggregato"
      , "giorno_sterilizzato"
      , "prelievo_non_sterilizzato_giorn_1"
      , "prelievo_non_sterilizzato_giorn_2"
      , "prelievo_non_sterilizzato_giorn_3"
      , "prelievo_non_sterilizzato_giorn_4"
      , "prelievo_non_sterilizzato_giorn_5"
      , "prelievo_non_sterilizzato_giorn_6"
      , "prelievo_non_sterilizzato_giorn_7"
      , "prelievo_non_sterilizzato_giorn_8"
      , "prelievo_non_sterilizzato_giorn_9"
      , "prelievo_non_sterilizzato_giorn_10"
      , "prelievo_non_sterilizzato_giorn_11"
      , "prelievo_non_sterilizzato_giorn_12"
      , "prelievo_non_sterilizzato_giorn_13"
      , "prelievo_non_sterilizzato_giorn_14"
      , "prelievo_non_sterilizzato_giorn_15"
      , "prelievo_non_sterilizzato_giorn_16"
      , "prelievo_non_sterilizzato_giorn_17"
      , "prelievo_non_sterilizzato_giorn_18"
      , "prelievo_non_sterilizzato_giorn_19"
      , "prelievo_non_sterilizzato_giorn_20"
      , "prelievo_non_sterilizzato_giorn_21"
      , "prelievo_non_sterilizzato_giorn_22"
      , "prelievo_non_sterilizzato_giorn_23"
      , "prelievo_non_sterilizzato_giorn_24"
      , "prelievo_non_sterilizzato_giorn_25"
      , "prelievo_non_sterilizzato_giorn_26"
      , "prelievo_non_sterilizzato_giorn_27"
      , "prelievo_non_sterilizzato_giorn_28"
      , "prelievo_non_sterilizzato_giorn_29"
      , "prelievo_non_sterilizzato_giorn_30"
      , "prelievo_non_sterilizzato_giorn_31")

    Assert.assertEquals(outFields.size, outFields.toSet.size)
    outFields.foreach(f => {
      println(f);
      Assert.assertTrue(outFieldsSet.contains(f))
    })
    Assert.assertEquals(outFieldsSet, outFields.toSet)
  }

}
