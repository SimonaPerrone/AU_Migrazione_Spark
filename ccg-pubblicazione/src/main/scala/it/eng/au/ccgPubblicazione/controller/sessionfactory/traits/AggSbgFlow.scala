package it.eng.au.ccgPubblicazione.controller.sessionfactory.traits

import it.eng.au.ccgPubblicazione.dao.rcugas.{RcugasPdrDao, RcugasVarMisuratoreDao}
import it.eng.au.ccgPubblicazione.schema.aggsbg.AggConsumptionRequestRunnableSchema
import it.eng.au.ccgPubblicazione.schema.rcugas.{RcugasPdrSchema, RcugasVarMisuratoreSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, lit, row_number, to_date, when}

trait AggSbgFlow extends SessionRun {
  val isDateInInterval = "is_date_in_interval"
  val rowNumber = "row_number"

  override def attachCoefficient(dailyConsumption: DataFrame): DataFrame = {
    val rcugasPdr = new RcugasPdrDao().readTable
    val rcugasVarMisuratore = new RcugasVarMisuratoreDao().readTable
      .withColumn(RcugasVarMisuratoreSchema.d_data_inizio, to_date(coalesce(col(RcugasVarMisuratoreSchema.d_data_inizio), lit("1970-01-01 00:00:00"))))
      .withColumn(RcugasVarMisuratoreSchema.d_data_fine, to_date(coalesce(col(RcugasVarMisuratoreSchema.d_data_fine), lit("2100-12-31 00:00:00"))))

    val dailyConsumptionWithNullCoefficient = dailyConsumption.where(col(AggConsumptionRequestRunnableSchema.coefficient).isNull)
    val dailyConsumptionWithValuedCoefficient = dailyConsumption.where(col(AggConsumptionRequestRunnableSchema.coefficient).isNotNull)

    getCoefficientFromRcugas(dailyConsumptionWithNullCoefficient, rcugasPdr, rcugasVarMisuratore)
      .selectExpr(fieldsConsumptionRequestRunnable: _*)
      .union(
        dailyConsumptionWithValuedCoefficient.selectExpr(fieldsConsumptionRequestRunnable: _*)
      )
      //we set the coefficient to 1.0 if it's 0 or less
      .withColumn(AggConsumptionRequestRunnableSchema.coefficient, when(col(AggConsumptionRequestRunnableSchema.coefficient) === lit(0.0), lit(1.0)).otherwise(col(AggConsumptionRequestRunnableSchema.coefficient)))
  }

  private def getCoefficientFromRcugas(dailyConsumption: DataFrame, rcugasPdr: DataFrame, rcugasVarMisuratore: DataFrame): DataFrame = {
    val windowByPdrAndDate = Window.partitionBy(AggConsumptionRequestRunnableSchema.pdr, AggConsumptionRequestRunnableSchema.date).orderBy(col(isDateInInterval).desc)

    dailyConsumption
      .join(rcugasPdr, dailyConsumption(AggConsumptionRequestRunnableSchema.pdr) === rcugasPdr(RcugasPdrSchema.t_codice_pdr), "inner")
      .join(rcugasVarMisuratore, rcugasPdr(RcugasPdrSchema.n_id_pdr) === rcugasVarMisuratore(RcugasVarMisuratoreSchema.n_id_pdr), "left")
      .withColumn(isDateInInterval,
        to_date(col(AggConsumptionRequestRunnableSchema.date)).between(col(RcugasVarMisuratoreSchema.d_data_inizio), col(RcugasVarMisuratoreSchema.d_data_fine))
      )
      .withColumn(rowNumber, row_number().over(windowByPdrAndDate))
      .where(col(rowNumber) === 1)
      .withColumn(AggConsumptionRequestRunnableSchema.coefficient, when(col(isDateInInterval), col(RcugasVarMisuratoreSchema.n_coeff_correzione)).otherwise(col(AggConsumptionRequestRunnableSchema.coefficient)))
  }
}
