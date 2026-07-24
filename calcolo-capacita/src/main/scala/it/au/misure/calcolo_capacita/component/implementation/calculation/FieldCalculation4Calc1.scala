package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculation4
import it.au.misure.calcolo_capacita.component.implementation.Transformation
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.typeCalc1Value
import it.au.misure.calcolo_capacita.component.utility.CalculatedField._
import it.au.misure.calcolo_capacita.component.utility.LoggerUtility
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ListUtility.ListUtility
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ProgressiveDay.ProgressiveDay
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.component.utility.property.ApplicationProperty.dateFormatToExport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}


object FieldCalculation4Calc1 extends FieldCalculation4 {


  private def calculateFromToPeriod(df: DataFrame): DataFrame = {
    df.withColumn(progressiveDayTodate, to_date(unix_timestamp(col(numericDay).cast(StringType), "yyyyMMdd").cast(TimestampType)))
      .withColumn(ClgPdrCapacitaSchema.d_data_da, date_format(col(progressiveDayTodate), dateFormatToExport))
      .withColumn(ClgPdrCapacitaSchema.d_data_a, expr("date_sub("+progressiveDayTodate+",cast("+numberDayConsecutiveAfterCurrentOnXWindow+" as int))"))
      .withColumn(ClgPdrCapacitaSchema.d_data_a, date_format(col(ClgPdrCapacitaSchema.d_data_a), dateFormatToExport))

  }

  private def calculateFirstAndLastDayOfMonth(df: DataFrame): DataFrame = {
    df
      .withColumn(lastDayOfMonth, last_day(col(annoMeseGiornoDate)))
      .withColumn(isLastDayOfMonth, when(col(annoMeseGiornoDate) === last_day(col(annoMeseGiornoDate)), true))
      .withColumn(firtDayOfMonth, trunc(col(annoMeseGiornoDate), "month"))
      .withColumn(isFirtDayOfMonth, when(col(annoMeseGiornoDate) === (col(firtDayOfMonth)), true))

  }

  private def calculateGap(df: DataFrame, wPartPdrOrderGiorno: WindowSpec): DataFrame = {
    df
      .withColumn(prev, lag(numericDay, 1, 0).over(wPartPdrOrderGiorno))
      .withColumn(diffCurrPrevDay, col(numericDay) - col(prev))

  }

  private def calculateInfraMese(df: DataFrame): DataFrame = {
    val wPartPdrOrderGiorno = Window.partitionBy(AnagraficaSchema.t_codice_pdr).orderBy(col(numericDay).desc)

    val df0 = df
      /*cattura il primo giorno in assoluto di tutto il range y*/
      .withColumn(diffCurrPrevDayUpdate, when((col(diffCurrPrevDay) === col(numericDay)), lit(-1)).otherwise(col(diffCurrPrevDay)))
    df0
      .withColumn(isInfraMese1, lag(isFirtDayOfMonth, 1, 0).over(wPartPdrOrderGiorno))
      .withColumn(diffCurrPrevDayUpdate, when((col(isInfraMese1) === lit(true)) and (col(isLastDayOfMonth) === lit(true)), lit(-1)).otherwise(col(diffCurrPrevDay)))
  }

  private def calculateMaxInFx(df: DataFrame,
                               wPartPdrOrderGiornoRangeNoCurrent: WindowSpec,
                               wPartPdrOrderGiornoRangeWithCurr: WindowSpec,
                               wPartPdrOrderGiorno: WindowSpec,
                               args: Args
                              ): DataFrame = {
    df
      .withColumn(intensitàGapBeforeCurrent, when((col(diffCurrPrevDayUpdate) === lit(-1)), lit(0)).otherwise(col(diffCurrPrevDayUpdate) * lit(-1)))
      .withColumn(numberDayConsecutiveAfterCurrentOnXWindow0, sum(when(col(intensitàGapBeforeCurrent) === lit(0), lit(1)).otherwise(lit(0))) over (wPartPdrOrderGiornoRangeNoCurrent))
      .withColumn(numberDayConsecutiveAfterCurrentOnXWindow, when(col(numberDayConsecutiveAfterCurrentOnXWindow0) isNull, lit(0)).otherwise(col(numberDayConsecutiveAfterCurrentOnXWindow0)))
      .withColumn(caMax, max(CalcoloConsumiSbgSchema.consumo) over (wPartPdrOrderGiornoRangeWithCurr))
      .withColumn(dayWithAtLeastXDayConsecutive, max(when(col(numberDayConsecutiveAfterCurrentOnXWindow) === lit(args.x - 1), col(numericDay)).otherwise(lit(0))) over (wPartPdrOrderGiorno))

  }

  private def calculateFieldToTake(df: DataFrame): DataFrame = {
    df
      .withColumn(toTake, when((col(dayWithAtLeastXDayConsecutive) - col(numericDay)) === lit(0), lit(true)).otherwise(lit(false)))

  }

  override def calculation(df: DataFrame)(implicit args: Args): DataFrame = {

    val wPartPdrOrderGiorno = Window.partitionBy(AnagraficaSchema.t_codice_pdr).orderBy(col(numericDay).desc)
    val wPartPdrOrderGiornoRangeNoCurrent = wPartPdrOrderGiorno.rowsBetween(1, args.x - 1)
    val wPartPdrOrderGiornoRangeWithCurr = wPartPdrOrderGiorno.rowsBetween(0, args.x - 1)

    val df_1 = df.getNumericDay()
    val df_v2 = calculateFirstAndLastDayOfMonth(df_1)
    val df_v3 = calculateGap(df_v2, wPartPdrOrderGiorno)
    val df_v4 = calculateInfraMese(df_v3)
    val df_v5 = calculateMaxInFx(df_v4,
      wPartPdrOrderGiornoRangeNoCurrent,
      wPartPdrOrderGiornoRangeWithCurr,
      wPartPdrOrderGiorno,
      args
    )
    val df_v5_2 = calculateFieldToTake(df_v5)
    val df_v6 = calculateFromToPeriod(df_v5_2)
    val df_v7 = df_v6.filter(col(toTake) === lit(true))
    if (args.verbose) {
      LoggerUtility.printInfo("I finished with tipCalc1 ", getClass.getName)
    }
    Transformation.setPcmFields(df_v7, typeCalc1Value)
  }

  override protected def getSchemaPreCalculation: List[String] =
    (CalcoloConsumiSbgSchema.getValues - CalcoloConsumiSbgSchema.cod_pdr) ::: List(annoMeseGiornoDate) :::
      AnagraficaSchema.getValues

  override protected def getFieldCalculated1: String = ClgPdrCapacitaSchema.n_pcm

  override protected def getFieldCalculated2: String = ClgPdrCapacitaSchema.t_tipo_calcolo

  override protected def getFieldCalculated3: String = ClgPdrCapacitaSchema.d_data_da

  override protected def getFieldCalculated4: String = ClgPdrCapacitaSchema.d_data_a

}
