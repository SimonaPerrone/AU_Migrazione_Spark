package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, UdbAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.util.Check
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}

import java.sql.Timestamp

class UdbAggregatorTest extends EnvironmentSparkTest with Check {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", "udd1", "pivaUdb1", "pivaIt", "pivaRdb", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "tipo-client1", "unit1", "pivaDistr1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "AGG_S1_PRE", false),
      ("pdr1", "udd1", "pivaUdb1", "pivaIt", "pivaRdb", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "tipo-client1", "unit1", "pivaDistr1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "AGG_S1_PRE", false),
      ("pdr1", "udd1", "pivaUdb1", "pivaIt", "pivaRdb", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "tipo-client1", "unit1", "pivaDistr1", "202105", Timestamp.valueOf("2021-05-15 00:00:00"), 1, 10, true, null, 1.0, "AGG_S1_PRE", false),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt", "pivaRdb", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202104", Timestamp.valueOf("2021-04-10 00:00:00"), 1, 10, true, "Y", 1.0, "AGG_S1_PRE", false),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt", "pivaRdb", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "AGG_S1_PRE", false),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt", "pivaRdb", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", 1.0, "AGG_S1_PRE", false)
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
        DailyConsumptionAggSchema.errorCode,
        DailyConsumptionAggSchema.isValid,
        DailyConsumptionAggSchema.treatment,
        DailyConsumptionAggSchema.valuef3,
        DailyConsumptionAggSchema.session,
        DailyConsumptionAggSchema.forcedExclusion
      )
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val result = UdbAggregator.getAggregato(df)
      .cache()

    result.show(false)
    var row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(UdbAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr2"))
      .collect()
    checkResultRowUDB(row(0), "202105", "01/05/2021", "pivaDistr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 2))
    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202104") and col(UdbAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr2"))
      .collect()
    checkResultRowUDB(row(0), "202104", "01/04/2021", "pivaDistr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 1))

    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(UdbAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr1"))
      .collect()
    checkResultRowUDB(row(0), "202105", "01/05/2021", "pivaDistr1", "udd1", "pivaUdb1", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "Y", "tipo-client1", "unit1",
      (10, 2))
    checkResultRowUDB(row(0), "202105", "01/05/2021", "pivaDistr1", "udd1", "pivaUdb1", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "Y", "tipo-client1", "unit1",
      (15, 1))
  }


}
