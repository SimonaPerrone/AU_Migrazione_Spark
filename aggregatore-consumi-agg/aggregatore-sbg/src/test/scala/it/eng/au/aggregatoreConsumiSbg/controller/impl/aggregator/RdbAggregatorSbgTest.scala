package it.eng.au.aggregatoreConsumiSbg.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, RdbAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiSbg.util.Check
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

import java.sql.Timestamp

class RdbAggregatorSbgTest extends EnvironmentSparkTest with Check {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb1", "it1"),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb1", "it1"),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-15 00:00:00"), 1, 1.0, 10, true, null, "rdb1", "it1"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202104", Timestamp.valueOf("2021-04-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb2", "it2"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb2", "it2"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y", "rdb2", "it2"),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-11 00:00:00"), 1, 1.0, 10, true, "Y", "rdb2", "10238291008")
    ).
      toDF(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.pivaUdd,
        DailyConsumptionAggSchema.pivaUdb,
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
        DailyConsumptionAggSchema.treatment,
        DailyConsumptionAggSchema.pivaRdb,
        DailyConsumptionAggSchema.pivaIt
      ).withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.session, lit("SBG"))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val result = RdbAggregatorSbg.getAggregato(df)
      .cache()

    result.show(false)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(1, result.filter(col(RdbAggregatorOutputSchema.PIVA_RDB) === "rdb1").count)
    Assert.assertEquals(2, result.filter(col(RdbAggregatorOutputSchema.PIVA_RDB) === "rdb2").count)


    var row = result
      .filter(col("annoMese") === lit("202105") and col(RdbAggregatorOutputSchema.PIVA_IT) === lit("it2"))
      .collect()
    checkResultRowRDB(row(0), "202105", "01/05/2021", "rdb2", "it2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 2))
    row = result
      .filter(col("annoMese") === lit("202104") and col(RdbAggregatorOutputSchema.PIVA_IT) === lit("it2"))
      .collect()
    checkResultRowRDB(row(0), "202104", "01/04/2021", "rdb2", "it2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 1))

    row = result
      .filter(col("annoMese") === lit("202105") and col(RdbAggregatorOutputSchema.PIVA_IT) === lit("it1"))
      .collect()
    checkResultRowRDB(row(0), "202105", "01/05/2021", "rdb1", "it1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "Y", "tipo-client", "unit",
      (10, 2))
    checkResultRowRDB(row(0), "202105", "01/05/2021", "rdb1", "it1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "Y", "tipo-client", "unit",
      (15, 1))

    Assert.assertEquals(result.filter(col("pivaIt") === "10238291008").count, 0)
  }
}
