package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, ItAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.util.Check
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}

import java.sql.Timestamp

class ItAggregatorTest extends EnvironmentSparkTest with Check {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "rdb1", "it1", "AGG_S1_PRE", false),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "rdb1", "it1", "AGG_S1_PRE", false),
      ("pdr1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "tipo-client", "unit", "pivaDistr", "202105", Timestamp.valueOf("2021-05-15 00:00:00"), 1, 10, true, null, 1.0, "rdb1", "it1", "AGG_S1_PRE", false),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202104", Timestamp.valueOf("2021-04-10 00:00:00"), 1, 10, true, "Y", 1.0, "rdb2", "it2", "AGG_S1_PRE", false),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "rdb2", "it2", "AGG_S1_PRE", false),
      ("pdr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "rdb2", "it2", "AGG_S1_PRE", false)
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
        DailyConsumptionAggSchema.errorCode,
        DailyConsumptionAggSchema.isValid,
        DailyConsumptionAggSchema.treatment,
        DailyConsumptionAggSchema.valuef3,
        DailyConsumptionAggSchema.pivaRdb,
        DailyConsumptionAggSchema.pivaIt,
        DailyConsumptionAggSchema.session,
        DailyConsumptionAggSchema.forcedExclusion
      )
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val result = ItAggregator.getAggregato(df)
      .cache()

    result.show(false)

    var row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(ItAggregatorOutputSchema.PIVA_IT) === lit("it2"))
      .collect()
    checkResultRowIT(row(0), "202105", "01/05/2021", "it2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 2))
    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202104") and col(ItAggregatorOutputSchema.PIVA_IT) === lit("it2"))
      .collect()
    checkResultRowIT(row(0), "202104", "01/04/2021", "it2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 1))

    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(ItAggregatorOutputSchema.PIVA_IT) === lit("it1"))
      .collect()
    checkResultRowIT(row(0), "202105", "01/05/2021", "it1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "Y", "tipo-client", "unit",
      (10, 2))
    checkResultRowIT(row(0), "202105", "01/05/2021", "it1", "udd1", "pivaUdb1", "dtg1", "cod_remi", "id-reg", "cod-prof", "Y", "tipo-client", "unit",
      (15, 1))
  }


}
