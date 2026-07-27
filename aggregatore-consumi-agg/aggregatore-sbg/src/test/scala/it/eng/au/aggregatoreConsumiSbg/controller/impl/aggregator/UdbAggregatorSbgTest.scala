package it.eng.au.aggregatoreConsumiSbg.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, UdbAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiSbg.util.Check
import org.apache.spark.sql.functions.{col, lit}

import java.io.DataOutput
import java.sql.Timestamp

class UdbAggregatorSbgTest extends EnvironmentSparkTest with Check {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", "udd1", "pivaUdb1", "pivaIt1", "pivaRdb", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y"),
      ("pdr1", "udd1", "pivaUdb1", "pivaIt1", "pivaRdb", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y"),
      ("pdr1", "udd1", "pivaUdb1", "pivaIt1", "pivaRdb", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-15 00:00:00"), 1, 1.0, 10, true, null),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt2", "pivaRdb", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202104", Timestamp.valueOf("2021-04-10 00:00:00"), 1, 1.0, 10, true, "Y"),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt2", "pivaRdb", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y"),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt2", "pivaRdb", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y")
    ).
      toDF(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.pivaUdd,
        DailyConsumptionAggSchema.pivaUdb,
        DailyConsumptionAggSchema.pivaIt,
        DailyConsumptionAggSchema.pivaRdb,
        DailyConsumptionAggSchema.dtg,
        DailyConsumptionAggSchema.codRemi,
        DailyConsumptionAggSchema.idRegClim,
        DailyConsumptionAggSchema.codProfStd,
        DailyConsumptionAggSchema.tipoCliente,
        DailyConsumptionAggSchema.unitMisPrel,
        DailyConsumptionAggSchema.pivaDistr,
        DailyConsumptionAggSchema.annoMese,
        DailyConsumptionAggSchema.date,
        DailyConsumptionAggSchema.value,
        DailyConsumptionAggSchema.valuef3,
        DailyConsumptionAggSchema.errorCode,
        DailyConsumptionAggSchema.isValid,
        DailyConsumptionAggSchema.treatment
      ).withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.session, lit("SBG"))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val result = UdbAggregatorSbg.getAggregato(df)
      .cache()

    result.show

    var row = result
      .filter(col("annoMese") === lit("202105") and col("PIVA_DISTR") === lit("pivaDistr2"))
      .collect()
    checkResultRowUDB(row(0), "202105", "01/05/2021", "pivaDistr2", "pivaUdb2", "udd2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 2))
    row = result
      .filter(col("annoMese") === lit("202104") and col("PIVA_DISTR") === lit("pivaDistr2"))
      .collect()
    checkResultRowUDB(row(0), "202104", "01/04/2021", "pivaDistr2", "pivaUdb2", "udd2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 1))

    row = result
      .filter(col("annoMese") === lit("202105") and col("PIVA_DISTR") === lit("pivaDistr"))
      .collect()
    checkResultRowUDB(row(0), "202105", "01/05/2021", "pivaDistr", "pivaUdb1", "udd1", "dtg1", "cod_remi", "id-reg", "cod-prof", "Y", "tipo-client", "unit",
      (10, 2))
    checkResultRowUDB(row(0), "202105", "01/05/2021", "pivaDistr", "pivaUdb1", "udd1", "dtg1", "cod_remi", "id-reg", "cod-prof", "Y", "tipo-client", "unit",
      (15, 1))
  }
}
