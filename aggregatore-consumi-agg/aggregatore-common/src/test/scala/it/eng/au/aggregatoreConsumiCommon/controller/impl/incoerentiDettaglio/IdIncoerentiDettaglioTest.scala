package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.IdPdrIncoerentiDettaglio
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class IdIncoerentiDettaglioTest extends EnvironmentSparkTest {
  def testRun(): Unit = {
    Environment.setProperty("incoerenzaGDM.numberOfDays.threshold", "5")
    Environment.setProperty("maxNumRowFile", "10")
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2020-12-08"), 1.0, 1.5, 100000.1, 0, true, "G", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-09"), 1.0, 1.5, 100000.1, 0, true, "G", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-10"), 1.0, 1.5, 100000.1, 0, true, "G", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-11"), 1.0, 1.5, 100000.1, 0, true, "G", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-12"), 1.0, 1.5, 100000.1, 0, true, "G", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-13"), 1.0, 1.5, 0.5, 0, true, "N", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G2,5", "202012"),
        (java.sql.Date.valueOf("2020-12-14"), 1.0, 1.5, 2.0, 0, true, "G", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G4", "202012"),
        (java.sql.Date.valueOf("2020-12-15"), 1.0, 1.5, 0.1, 10, true, "G", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G60", "202012"),
        (java.sql.Date.valueOf("2020-12-16"), 1.0, 1.5, 0.1, 10, true, "G", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-17"), 1.0, 1.5, 100000.1, 10, true, "G", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-18"), 1.0, 1.5, 100000.1, 10, true, "G", "002PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2020-12-19"), 1.0, 1.5, 100000.1, 10, true, "G", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202012"),
        (java.sql.Date.valueOf("2021-12-19"), 1.0, 1.5, 100000.1, 10, true, "G", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "G1,6", "202112")
      )
    ).toDF(
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.coefficient,
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.ca,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.classeMisuratore,
      DailyConsumptionAggSchema.annoMese)
      .withColumn(DailyConsumptionAggSchema.idFormula, lit("1"))
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, lit("/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml"))
      .withColumn(DailyConsumptionAggSchema.rightMeasureLocalFile, lit("/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._dx.zml"))
      .withColumn(DailyConsumptionAggSchema.session, lit("AGG_S1_PRE"))
      .withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.dtg, lit("Y"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))
      .withColumn("valueNotSterilizedI", lit(0.0))


    val validate = Environment.sparkContext.parallelize(
      List(
        ("000PDR", java.sql.Date.valueOf("2020-12-11"), "IGMGPRE", true, "/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml", 1622211816719L)
        , ("000PDR", java.sql.Date.valueOf("2020-12-14"), "RGL", false, "/mnt/isilon/piva11111111_piva1234/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml", 1622211816719L)
        , ("000PDR", java.sql.Date.valueOf("2020-12-15"), "TGL", false, "/mnt/isilon/piva11111111_piva1234/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml", 1622211816719L)
      )
    ).toDF(ValidatedFlowsAggSchema.getValues: _*)

    /*val validateTable = Environment.getValidatedFlowTableName
    Environment.sqlContext.sql("drop database if exists test cascade")
    Environment.sqlContext.sql("create database if not exists test")
    Environment.sqlContext.sql(s"create external table $validateTable (pdr string, date timestamp, service string, iscorrected boolean, localfile string, executionid bigint) location 'src/test/resources/hdfs/validated_flows'")
    validate.write.insertInto(validateTable)*/

    val anomalousPdrs = IdIncoerentiDettaglio.getAnomalousPdrs(dailyConsumptionDF)
    val outDF = IdPdrIncoerentiDettaglio.getAggregatoFromPdrs(anomalousPdrs)

    //IdIncoerentiDettaglio.run(dailyConsumptionDF)

    outDF.show(truncate = false)
    outDF.na.fill("").show(truncate = false)
    outDF.where(col("prelievo_non_sterilizzato_giorn_1").isNull).show(truncate = false)

    //Assert.assertEquals(1, outDF.filter(col("prelievo_non_sterilizzato_giorn_17") === lit(100000)).count)

    val outFields = IdPdrIncoerentiDettaglio.csvFields
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

    //IdIncoerentiDettaglio.run(dailyConsumptionDF)
  }
}
