package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.EnvironmentSparkTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DecimalType

class QueryIncoerentiTest extends EnvironmentSparkTest {
  def testGetQuery(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (java.sql.Date.valueOf("2020-12-12"), 100000.1, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", null),
        (java.sql.Date.valueOf("2020-12-13"), 0.5, 0, true, "N", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", null),
        (java.sql.Date.valueOf("2020-12-14"), 2.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", null),
        (java.sql.Date.valueOf("2020-12-15"), 0.1, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "0000PIVARDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", "sm3", null)
      )
    )
      .toDF(
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
        DailyConsumptionAggSchema.pivaRdb,
        DailyConsumptionAggSchema.dtg,
        DailyConsumptionAggSchema.codRemi,
        DailyConsumptionAggSchema.ca,
        DailyConsumptionAggSchema.idRegClim,
        DailyConsumptionAggSchema.codProfStd,
        DailyConsumptionAggSchema.tipoCliente,
        DailyConsumptionAggSchema.causale,
        DailyConsumptionAggSchema.session,
        DailyConsumptionAggSchema.unitMisPrel,
        DailyConsumptionAggSchema.forcedExclusion
      )
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1000.0)) //in questo modo i PdR non possono essere incoerenti GDM
      .withColumn(DailyConsumptionAggSchema.ca, col(DailyConsumptionAggSchema.ca).cast(DecimalType(12, 1)))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))
      .withColumn("valueNotSterilizedI", lit(1.0))


    val aggregatoDf = QueryIncoerenti.getQueryDF(dailyConsumptionDF)
    aggregatoDf.show(10, truncate = false)
    aggregatoDf.printSchema()
  }
}
