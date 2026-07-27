package it.eng.au.aggregatoreConsumiCommon.controller

import it.eng.au.aggregatoreConsumiCommon.dao.rcugas.{RcugasPdrDao, RcugasVarMisuratoreDao}
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema, RcugasPdrSchema, RcugasVarMisuratoreSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Si occupa del popolamento del campo `coefficient` nella tabella dei consumi ove non presente.
 * Tale campo è utilizzato per identificare i PdR incoerenti GDM. */
object CoefficientController {
  val isDateInInterval = "is_date_in_interval"
  val rowNumber = "row_number"

  /**
   * Aggiorna il coefficiente nella tabella dei consumi ove non presente. Inoltre, setta il coefficiente a 1.0 se è presente un coefficiente nullo.
   * @param dailyConsumption tabella dei consumi
   * @return tabella dei consumi con i coefficienti aggiornati.
   */
  def attachCoefficient(dailyConsumption: DataFrame): DataFrame = {
    val rcugasPdr = new RcugasPdrDao().readTable
    val rcugasVarMisuratore = new RcugasVarMisuratoreDao().readTable
      .withColumn(RcugasVarMisuratoreSchema.d_data_inizio, to_date(coalesce(col(RcugasVarMisuratoreSchema.d_data_inizio), lit("1970-01-01 00:00:00"))))
      .withColumn(RcugasVarMisuratoreSchema.d_data_fine, to_date(coalesce(col(RcugasVarMisuratoreSchema.d_data_fine), lit("2100-12-31 00:00:00"))))

    val dailyConsumptionWithNullCoefficient = dailyConsumption.where(col(DailyConsumptionAggSchema.coefficient).isNull)
    val dailyConsumptionWithValuedCoefficient = dailyConsumption.where(col(DailyConsumptionAggSchema.coefficient).isNotNull)

    getCoefficientFromRcugas(dailyConsumptionWithNullCoefficient, rcugasPdr, rcugasVarMisuratore)
      .selectExpr(DailyConsumptionInputProcessSchema.getValues: _*)
      .union(dailyConsumptionWithValuedCoefficient.selectExpr(DailyConsumptionInputProcessSchema.getValues: _*))
      //we set the coefficient to 1.0 if it's 0 or less
      .withColumn(DailyConsumptionAggSchema.coefficient, when(col(DailyConsumptionAggSchema.coefficient) === lit(0.0), lit(1.0)).otherwise(col(DailyConsumptionAggSchema.coefficient)))
  }

  /**
   * Estrae il coefficiente di correzione della [[rcugasVarMisuratore]] e lo sovrascrive nella [[dailyConsumption]] se vi è corrispondenza tra i PdR e le date
   * (la data della misura deve cadere nell'intervallo presente in [[rcugasVarMisuratore]]).
   * @param dailyConsumption tabella dei consumi
   * @param rcugasPdr tabella contenente la relazione t_codice_pdr <-> n_id_pdr
   * @param rcugasVarMisuratore tabella contenente i coefficienti di correzione
   * @return tabella dei consumi con i coefficienti aggiornati
   */
  def getCoefficientFromRcugas(dailyConsumption: DataFrame, rcugasPdr: DataFrame, rcugasVarMisuratore: DataFrame): DataFrame = {
    val windowByPdrAndDate = Window.partitionBy(DailyConsumptionAggSchema.pdr, DailyConsumptionAggSchema.date).orderBy(col(isDateInInterval).desc)

    dailyConsumption
      .join(rcugasPdr, dailyConsumption(DailyConsumptionAggSchema.pdr) === rcugasPdr(RcugasPdrSchema.t_codice_pdr), "inner")
      .join(rcugasVarMisuratore, rcugasPdr(RcugasPdrSchema.n_id_pdr) === rcugasVarMisuratore(RcugasVarMisuratoreSchema.n_id_pdr), "left")
      .withColumn(isDateInInterval,
        to_date(col(DailyConsumptionAggSchema.date)).between(col(RcugasVarMisuratoreSchema.d_data_inizio), col(RcugasVarMisuratoreSchema.d_data_fine))
      )
      .withColumn(rowNumber, row_number().over(windowByPdrAndDate))
      .where(col(rowNumber) === 1)
      .withColumn(DailyConsumptionAggSchema.coefficient, when(col(isDateInInterval), col(RcugasVarMisuratoreSchema.n_coeff_correzione)).otherwise(col(DailyConsumptionAggSchema.coefficient)))
  }
}
