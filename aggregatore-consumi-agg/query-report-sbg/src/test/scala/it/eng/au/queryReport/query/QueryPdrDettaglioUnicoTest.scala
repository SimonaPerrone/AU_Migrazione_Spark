package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.EnvironmentSparkTest
import it.eng.au.queryReport.query.dettaglioUnico.QueryPdrDettaglioUnico
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DecimalType

class QueryPdrDettaglioUnicoTest extends EnvironmentSparkTest {
  def testAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (date, 0.4, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.5, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.2, 11, true, "G", "202012", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1, 0, true, "G", "202012", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1, 6, false, "N", "202012", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1")
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
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.session
    ).withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.pivaRdb, lit("000RDB"))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn(DailyConsumptionAggSchema.ca, col(DailyConsumptionAggSchema.ca).cast(DecimalType(12, 1)))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val pdrDettaglioUnico = QueryPdrDettaglioUnico.getQueryDF(dailyConsumptionDF)
    pdrDettaglioUnico.show()
    pdrDettaglioUnico.printSchema()
  }
}
