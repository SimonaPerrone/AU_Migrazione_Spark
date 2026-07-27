package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioUnicoSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}
import org.junit.Assert

import scala.collection.immutable.ListMap

class UddElencoFlussiDettaglioUnicoTest extends EnvironmentSparkTest with ElencoFlussiDettaglioUnico {
  def testAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (date, 0.4, 1.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1),
        (date, 0.5, 1.0, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1),
        (date, 0.2, 1.0, 11, true, "G", "202012", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1),
        (date, 0.1, 1.0, 0, true, "G", "202012", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1),
        (date, 0.1, 1.0, 6, false, "N", "202012", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
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
      DailyConsumptionAggSchema.session,
      DailyConsumptionAggSchema.idFormula
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, lit("/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml"))
      .withColumn(DailyConsumptionAggSchema.rightMeasureLocalFile, lit("file_dx"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))


    val validate = Environment.sparkContext.parallelize(
      List(
        ("000PDR", date, "IGMGPRE", true, "/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml", "123")
        , ("000PDR", date, "RGL", false, "/mnt/isilon/piva11111111_piva1234/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml", "123")
      )
    ).toDF(ValidatedFlowsAggSchema.getValues: _*)
      .drop(col(ValidatedFlowsAggSchema.executionid))

    val aggDF = getAggregato(dailyConsumptionDF, validate).persist()
    aggDF.show()

    Assert.assertEquals(5, aggDF.count())
    Assert.assertEquals(3, aggDF.filter(col(DettaglioUnicoSchema.pdr) === lit("000PDR") and col(DettaglioUnicoSchema.Prelievo) === lit(1)).count)
    Assert.assertEquals(2, aggDF.filter(col(DettaglioUnicoSchema.pdr) === lit("001PDR") and col(DettaglioUnicoSchema.Prelievo) === lit(0)).count)
    Assert.assertEquals(0, aggDF.filter(col(DettaglioUnicoSchema.pdr) === lit("002PDR")).count)

    val csvColsNameSet = Set("pdr"
      , "Prelievo"
      , "Trattamento"
      , "Nome_file"
      , "Sessione"
      , "Annomese")

    val csvFields = UddElencoFlussiDettaglioUnico.getCsvFields(aggDF)

    Assert.assertEquals(csvColsNameSet.size, csvColsNameSet.intersect(csvFields.toSet).size)
    Assert.assertEquals(csvColsNameSet.size, csvFields.toSet.size)

  }

  override val keyPiva1: String = DettaglioUnicoSchema.piva_udd
  override val keyPiva2: String = DettaglioUnicoSchema.piva_distr
  override val mainPiva: String = keyPiva2
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> DettaglioUnicoSchema.pdr.toString,
    DailyConsumptionAggSchema.value.toString -> DettaglioUnicoSchema.Prelievo.toString,
    DailyConsumptionAggSchema.treatment.toString -> DettaglioUnicoSchema.Trattamento.toString,
    DailyConsumptionAggSchema.leftMeasureLocalFile.toString -> DettaglioUnicoSchema.Nome_file.toString,
    DailyConsumptionAggSchema.session.toString -> DettaglioUnicoSchema.Sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> DettaglioUnicoSchema.Annomese.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> DettaglioUnicoSchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> DettaglioUnicoSchema.piva_distr.toString
  )
  val baseName: String = "AGG1"
  override val baseNumber: String = "1"

  override def getCsvFields(dfAggregato: DataFrame): List[String] = dfAggregato.columns.toList.diff(List(keyPiva1, keyPiva2))

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdd).isNotNull and col(DailyConsumptionAggSchema.pivaDistr).isNotNull

}
