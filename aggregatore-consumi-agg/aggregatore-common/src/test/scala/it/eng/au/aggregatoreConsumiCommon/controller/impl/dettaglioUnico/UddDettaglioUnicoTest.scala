package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{ceil, col, lit, monotonically_increasing_id}
import org.apache.spark.sql.types.StringType

import java.sql.Timestamp
import java.time.LocalDateTime

class UddDettaglioUnicoTest extends EnvironmentSparkTest {
  def testRun(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    val date = java.sql.Date.valueOf("2020-12-12")
    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        (date, 0.4, 1.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.5, 1.0, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.2, 1.0, 11, true, "G", "202012", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 0, true, "G", "202012", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 6, false, "N", "202012", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.4, 1.0, 0, true, "G", "202011", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.5, 1.0, 10, true, "G", "202011", "000PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.2, 1.0, 11, true, "G", "202011", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 0, true, "G", "202011", "001PDR", "001DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 6, false, "N", "202011", "002PDR", "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)

        , (date, 0.4, 1.0, 0, true, "G", "202012", "000PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.5, 1.0, 10, true, "G", "202012", "000PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.2, 1.0, 11, true, "G", "202012", "001PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 0, true, "G", "202012", "001PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 6, false, "N", "202012", "002PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.4, 1.0, 0, true, "G", "202011", "000PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.5, 1.0, 10, true, "G", "202011", "000PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.2, 1.0, 11, true, "G", "202011", "001PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 0, true, "G", "202011", "001PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 6, false, "N", "202011", "002PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "000REMI", 0.1, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)

        , (date, 0.4, 1.0, 0, true, "G", "202012", "100PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.5, 1.0, 10, true, "G", "202012", "100PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.2, 1.0, 11, true, "G", "202012", "101PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 0, true, "G", "202012", "101PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 6, false, "N", "202012", "102PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.4, 1.0, 0, true, "G", "202011", "100PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.5, 1.0, 10, true, "G", "202011", "100PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.2, 1.0, 11, true, "G", "202011", "101PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 0, true, "G", "202011", "101PDR", "001DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)
        , (date, 0.1, 1.0, 6, false, "N", "202011", "102PDR", "000DISTR", "000IT", "001UDD", "000UDB", "000RDB", "Y", "100REMI", 0.1, "100IDCL", "CODPROF_0", "U", "T", "S_AGG_1", 1)

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
      .withColumn(DailyConsumptionAggSchema.rightMeasureLocalFile, lit("/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._dx.zml"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    Environment.setProperty("date.run", Timestamp.valueOf(LocalDateTime.now()).toString)

    UddDettaglioUnico.runTest(dailyConsumptionDF).count
  }
}
