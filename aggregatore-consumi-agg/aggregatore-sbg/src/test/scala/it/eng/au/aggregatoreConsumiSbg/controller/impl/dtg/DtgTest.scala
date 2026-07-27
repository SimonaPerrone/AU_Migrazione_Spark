package it.eng.au.aggregatoreConsumiSbg.controller.impl.dtg

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import org.apache.spark.sql.functions.lit

class DtgTest extends EnvironmentSparkTest {
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
        (date, 0.1, 1.0, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.1122, 1.0, 10, true, "G", "202012", "1", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 0.6289, 1.0, 10, true, "G", "202012", "2", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 1.1242, 1.0, 10, true, "G", "202012", "3", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 1.92799999, 1.0, 10, true, "G", "202012", "4", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 1.925, 1.0, 10, true, "G", "202012", "5", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 23.342, 1.0, 10, true, "G", "202012", "6", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 23.347, 1.0, 10, true, "G", "202012", "7", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 23.345, 1.0, 10, true, "G", "202012", "8", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 23.346, 1.0, 10, true, "G", "202012", "9", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 23.3451, 1.0, 10, true, "G", "202012", "10", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 23.34501, 1.0, 10, true, "G", "202012", "11", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1"),
        (date, 23.34500, 1.0, 10, true, "G", "202012", "12", "000DISTR", "000IT", "000UDD", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1")
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

    val outDF = DtgSbg.getAggregato(dailyConsumptionDF)
      .cache

    outDF.show
  }
}
