package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.EnvironmentSparkTest
import it.eng.au.queryReport.query.dettaglioIncoerenti.QueryPdrDettaglioIncoerenti
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DecimalType

class QueryDettaglioIncoerentiTest extends EnvironmentSparkTest {
  def testGetQueryPdr(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2020-12-12"), 100000.1, 1.5, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3"),
        (java.sql.Date.valueOf("2020-12-13"), 0.5, 1.5, 0, true, "N", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3"),
        (java.sql.Date.valueOf("2020-12-14"), 2.0, 1.5, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3"),
        (java.sql.Date.valueOf("2020-12-15"), 0.1, 1.5, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3")
      )
    )
      .toDF(
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
        DailyConsumptionAggSchema.unitMisPrel
      )
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(false))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G60"))
      .withColumn(DailyConsumptionAggSchema.ca, col(DailyConsumptionAggSchema.ca).cast(DecimalType(12, 1)))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))
      .withColumn("valueNotSterilizedI", lit(1.0))

    val aggregatoDf = QueryPdrDettaglioIncoerenti.getQueryDF(dailyConsumptionDF)
    aggregatoDf.printSchema()
    aggregatoDf.show(10, truncate = false)
  }
}
