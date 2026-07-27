package it.eng.au.aggregatoreConsumiCommon.controller

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, RcugasPdrSchema, RcugasVarMisuratoreSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}

class CoefficientControllerTest extends EnvironmentSparkTest {
  def testAttachCoefficient(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumption = Seq(
      ("pdr1", "2022-05-01 00:00:00", 1.05),
      ("pdr1", "2022-05-02 00:00:00", 1.05),
      ("pdr1", "2022-05-03 00:00:00", 1.05),

      ("pdr2", "2022-05-01 00:00:00", 1.05),
      ("pdr2", "2022-05-02 00:00:00", 1.05),
      ("pdr2", "2022-05-03 00:00:00", 1.05),
      ("pdr2", "2022-05-04 00:00:00", 1.05),
      ("pdr2", "2022-05-05 00:00:00", 1.05),

      ("pdr3", "2022-05-01 00:00:00", 1.05),
      ("pdr3", "2022-05-02 00:00:00", 1.05),
      ("pdr3", "2022-05-03 00:00:00", 1.05)
    ).toDF(DailyConsumptionAggSchema.pdr, DailyConsumptionAggSchema.date, DailyConsumptionAggSchema.coefficient)

    val rcugasPdr = Seq(
      ("pdr1", "nIdPdr1"),
      ("pdr2", "nIdPdr2"),
      ("pdr3", "nIdPdr3")
    ).toDF(RcugasPdrSchema.t_codice_pdr, RcugasPdrSchema.n_id_pdr)

    val rcugasVarMisuratore = Seq(
      ("nIdPdr1", "2022-05-01 00:00:00.0", null, 1.1),
      ("nIdPdr2", "2022-05-01 00:00:00.0", "2022-05-03 00:00:00.0", 1.2),
      ("nIdPdr2", "2022-05-04 00:00:00.0", null, 1.3),
      ("nIdPdr3", "2022-05-05 00:00:00.0", null, 1.4)
    ).toDF(RcugasVarMisuratoreSchema.n_id_pdr, RcugasVarMisuratoreSchema.d_data_inizio, RcugasVarMisuratoreSchema.d_data_fine, RcugasVarMisuratoreSchema.n_coeff_correzione)
      .withColumn(RcugasVarMisuratoreSchema.d_data_inizio, to_date(coalesce(col(RcugasVarMisuratoreSchema.d_data_inizio), lit("1970-01-01 00:00:00"))))
      .withColumn(RcugasVarMisuratoreSchema.d_data_fine, to_date(coalesce(col(RcugasVarMisuratoreSchema.d_data_fine), lit("2100-12-31 00:00:00"))))

    rcugasVarMisuratore.show

    CoefficientController.getCoefficientFromRcugas(dailyConsumption, rcugasPdr, rcugasVarMisuratore)
      .orderBy(DailyConsumptionAggSchema.pdr, DailyConsumptionAggSchema.date)
      .show
  }
}
