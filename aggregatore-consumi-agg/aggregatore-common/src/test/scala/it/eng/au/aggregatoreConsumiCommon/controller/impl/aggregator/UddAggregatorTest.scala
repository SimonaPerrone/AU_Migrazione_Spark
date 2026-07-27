package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.lit
import org.junit.Assert

import java.sql.Timestamp

class UddAggregatorTest extends EnvironmentSparkTest {
  def testRun(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      List(
        ("pdr1", 0, true, "G", "202001", 0.1, "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", "000IDCL", "CODPROF_0", "U", "T", 1.0, "AGG_S1_PRE"),
        ("pdr1", 0, true, "G", "202001", 0.1, "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", "000IDCL", "CODPROF_0", "U", "T", 1.0, "AGG_S1_PRE"),
        ("pdr1", 0, true, "G", "202001", 0.1, "001DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", "000IDCL", "CODPROF_0", "U", "T", 1.0, "AGG_S1_PRE"),

        ("pdr1", 0, false, "G", "202001", 0.1, "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", "000IDCL", "CODPROF_0", "U", "T", 1.0, "AGG_S1_PRE"),
        ("pdr1", 0, true, "N", "202001", 0.1, "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", "000IDCL", "CODPROF_0", "U", "T", 1.0, "AGG_S1_PRE"),
        ("pdr1", 0, true, null, "202001", 0.1, "000DISTR", "000IT", "000UDD", "000UDB", "000RDB", "Y", "000REMI", "000IDCL", "CODPROF_0", "U", "T", 1.0, "AGG_S1_PRE")
      )
    ).toDF(
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.pivaRdb,
      DailyConsumptionAggSchema.dtg,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.session
    ).withColumn(DailyConsumptionAggSchema.date, lit(Timestamp.valueOf("2020-01-01 00:00:00")))
      .withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("sm3"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

     UddAggregator.run(dailyConsumptionDF)
  }
}
