package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioUnicoSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class IdPdrDettaglioUnicoTest extends EnvironmentSparkTest {
  def testAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (date, 0.4, 1.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.5, 1.0, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.2, 1.0, 11, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1, 1.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1, 1.0, 6, false, "N", "202012", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1")
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
      DailyConsumptionAggSchema.pivaUdb,
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
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val aggDF = IdPdrDettaglioUnico.getAggregato(dailyConsumptionDF).persist()
    aggDF.show()

    Assert.assertEquals(2, aggDF.count())
    Assert.assertEquals(2, aggDF.filter(col(DettaglioUnicoSchema.pdr) === lit("000PDR") and col(DettaglioUnicoSchema.Prelievo) === lit(1)).count)
    Assert.assertEquals(0, aggDF.filter(col(DettaglioUnicoSchema.pdr) === lit("002PDR")).count)

    val csvColsNameSet = Set("pdr"
      , "Prelievo"
      , "Piva_Udb"
      , "Dtg"
      , "Cod_Remi"
      , "Id_regione_climatica"
      , "Cod_prof_prel_std"
      , "Trattamento"
      , "Tipo_cliente"
      , "Sessione"
      , "Annomese")

    val csvFields = IdPdrDettaglioUnico.getCsvFields(aggDF)

    Assert.assertEquals(csvColsNameSet.size, csvColsNameSet.intersect(csvFields.toSet).size)
    Assert.assertEquals(csvColsNameSet.size, csvFields.toSet.size)

  }

}
