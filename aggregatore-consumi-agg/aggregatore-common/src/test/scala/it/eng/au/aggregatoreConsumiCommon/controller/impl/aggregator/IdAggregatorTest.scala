package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, IdAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.util.Check
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

import java.sql.Timestamp

class IdAggregatorTest extends EnvironmentSparkTest with Check {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = Environment.sparkContext.parallelize(List(
      ("pdr1", "udd1", "pivaUdb1", "pivaIt1", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "tipo-client1", "unit1", "pivaDistr1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", Some(1.0), false),
      ("pdr1", "udd1", "pivaUdb1", "pivaIt1", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "tipo-client1", "unit1", "pivaDistr1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", Some(1.0), false),
      ("pdr1", "udd1", "pivaUdb1", "pivaIt1", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "tipo-client1", "unit1", "pivaDistr1", "202105", Timestamp.valueOf("2021-05-15 00:00:00"), 1, 10, true, null, Some(1.0), false),

      ("pdr2", "udd2", "pivaUdb2", "pivaIt2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202104", Timestamp.valueOf("2021-04-10 00:00:00"), 1, 10, true, "Y", Some(1.0), false),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", Some(1.0), false),
      ("pdr2", "udd2", "pivaUdb2", "pivaIt2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "tipo-client2", "unit2", "pivaDistr2", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 10, true, "Y", Some(1.0), false)) ++

      (1 to 31).map(i => ("pdr3", "udd3", "pivaUdb3", "pivaIt3", "dtg3", "cod_remi3", "id-reg3", "cod-prof3", "tipo-client3", "unit3", "pivaDistr3", "202105", Timestamp.valueOf(s"2021-05-$i 00:00:00"), 1, 10, true, "Y", Some(1.0), false)) ++

      List(
        ("pdr4", "udd4", "pivaUdb4", "pivaIt4", "dtg4", "cod_remi4", "id-reg4", "cod-prof4", "tipo-client4", "unit4", "pivaDistr4", "202105", Timestamp.valueOf("2021-05-01 00:00:00"), 1, 5, true, "Y", Some(1.0), true),
        ("pdr4", "udd4", "pivaUdb4", "pivaIt4", "dtg4", "cod_remi4", "id-reg4", "cod-prof4", "tipo-client4", "unit4", "pivaDistr4", "202105", Timestamp.valueOf("2021-05-02 00:00:00"), 1, 5, true, "Y", Some(1.0), false),
        ("pdr4", "udd4", "pivaUdb4", "pivaIt4", "dtg4", "cod_remi4", "id-reg4", "cod-prof4", "tipo-client4", "unit4", "pivaDistr4", "202105", Timestamp.valueOf("2021-05-03 00:00:00"), 1, 5, true, "Y", None, false),

        ("pdr5", "udd5", "pivaUdb5", "pivaIt5", "dtg5", "cod_remi5", "id-reg5", "cod-prof5", "tipo-client5", "unit5", "pivaDistr5", "202105", Timestamp.valueOf("2021-05-01 00:00:00"), 1, 10, true, "Y", Some(1.0), false),
        ("pdr5", "udd5", "pivaUdb5", "pivaIt5", "dtg5", "cod_remi5", "id-reg5", "cod-prof5", "tipo-client5", "unit5", "pivaDistr5", "202105", Timestamp.valueOf("2021-05-02 00:00:00"), 1, 10, true, "Y", Some(1.0), false),
        ("pdr5", "udd5", "pivaUdb5", "pivaIt5", "dtg5", "cod_remi5", "id-reg5", "cod-prof5", "tipo-client5", "unit5", "pivaDistr5", "202105", Timestamp.valueOf("2021-05-03 00:00:00"), 1, 10, true, "Y", None, false),

        ("pdr6", "udd6", "pivaUdb6", "pivaIt6", "dtg6", "cod_remi6", "id-reg6", "cod-prof6", "tipo-client6", "unit6", "pivaDistr6", "202105", Timestamp.valueOf("2021-05-03 00:00:00"), 1, 5, true, "Y", None, true),
        ("pdr6", "udd6", "pivaUdb6", "pivaIt6", "dtg6", "cod_remi6", "id-reg6", "cod-prof6", "tipo-client6", "unit6", "pivaDistr6", "202105", Timestamp.valueOf("2021-05-03 00:00:00"), 1, 5, true, "Y", None, true),
        ("pdr6", "udd6", "pivaUdb6", "pivaIt6", "dtg6", "cod_remi6", "id-reg6", "cod-prof6", "tipo-client6", "unit6", "pivaDistr6", "202105", Timestamp.valueOf("2021-05-03 00:00:00"), 1, 5, true, "Y", None, true)
      )).
      toDF(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.pivaUdd,
        DailyConsumptionAggSchema.pivaUdb,
        DailyConsumptionAggSchema.pivaIt,
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
        DailyConsumptionAggSchema.forcedExclusion
      )
      .withColumn(DailyConsumptionAggSchema.pivaRdb, lit("pivaRdb"))
      .withColumn(DailyConsumptionAggSchema.session, lit("AGG_S1_PRE"))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    val result = IdAggregator.getAggregato(df)
      .cache()

    result.show(false)

    Assert.assertEquals(1, result.filter(col(DailyConsumptionAggSchema.pivaDistr) === "pivaDistr1").count)
    Assert.assertEquals(2, result.filter(col(DailyConsumptionAggSchema.pivaDistr) === "pivaDistr2").count)
    Assert.assertEquals(1, result.filter(col(DailyConsumptionAggSchema.pivaDistr) === "pivaDistr3").count)
    Assert.assertEquals(0, result.filter(col(DailyConsumptionAggSchema.pivaDistr) === "pivaDistr4").count)
    Assert.assertEquals(1, result.filter(col(DailyConsumptionAggSchema.pivaDistr) === "pivaDistr5").count)
    Assert.assertEquals(0, result.filter(col(DailyConsumptionAggSchema.pivaDistr) === "pivaDistr6").count)

    var row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(IdAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr2"))
      .collect()
    checkResultRowID(row(0), "202105", "01/05/2021", "pivaDistr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 2))
    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202104") and col(IdAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr2"))
      .collect()
    checkResultRowID(row(0), "202104", "01/04/2021", "pivaDistr2", "udd2", "pivaUdb2", "dtg2", "cod_remi2", "id-reg2", "cod-prof2", "Y", "tipo-client2", "unit2",
      (10, 1))

    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(IdAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr1"))
      .collect()
    checkResultRowID(row(0), "202105", "01/05/2021", "pivaDistr1", "udd1", "pivaUdb1", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "Y", "tipo-client1", "unit1",
      (10, 2))
    checkResultRowID(row(0), "202105", "01/05/2021", "pivaDistr1", "udd1", "pivaUdb1", "dtg1", "cod_remi1", "id-reg1", "cod-prof1", "Y", "tipo-client1", "unit1",
      (15, 1))

    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(IdAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr3"))
      .collect()
    checkResultRowID(row(0), "202105", "01/05/2021", "pivaDistr3", "udd3", "pivaUdb3", "dtg3", "cod_remi3", "id-reg3", "cod-prof3", "Y", "tipo-client3", "unit3",
      (16, 1))

    row = result
      .filter(col(DailyConsumptionAggSchema.annoMese) === lit("202105") and col(IdAggregatorOutputSchema.PIVA_DISTR) === lit("pivaDistr5"))
      .collect()
    checkResultRowID(row(0), "202105", "01/05/2021", "pivaDistr5", "udd5", "pivaUdb5", "dtg5", "cod_remi5", "id-reg5", "cod-prof5", "Y", "tipo-client5", "unit5",
      (2, 1))
  }
}
