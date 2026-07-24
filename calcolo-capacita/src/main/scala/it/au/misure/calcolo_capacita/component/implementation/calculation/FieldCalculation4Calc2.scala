package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculation4
import it.au.misure.calcolo_capacita.component.implementation.Transformation
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.typeCalc2Value
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{annoMeseGiornoDate, caMax, numericDay}
import it.au.misure.calcolo_capacita.component.utility.LoggerUtility
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ListUtility.ListUtility
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ProgressiveDay.ProgressiveDay
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.component.utility.property.ApplicationProperty.dateFormatToExport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}

object FieldCalculation4Calc2 extends FieldCalculation4 {

  private def calculateDaAPeriod(df: DataFrame): DataFrame = {
    val window = Window.partitionBy(AnagraficaSchema.t_codice_pdr).orderBy(col(numericDay).desc)

    /*fixme: per ora ha senso implementarlo così. così in fase di test campiamo
    *  se sono giusti i dati su cui ha fatto il max, poi una volta che siamo certi che l'algoritmo funzioni
    * questi periodi li calcoliamo in modo statico*/
    val dfPdrA = df
      .withColumn("last", lead(numericDay, 1) over window)
      .filter(col("last").isNull)
      .withColumn(ClgPdrCapacitaSchema.d_data_a, col(numericDay))
      .withColumn(ClgPdrCapacitaSchema.d_data_a, to_date(unix_timestamp(col(ClgPdrCapacitaSchema.d_data_a).cast(StringType), "yyyyMMdd").cast(TimestampType)))
      .withColumn(ClgPdrCapacitaSchema.d_data_a, date_format(col(ClgPdrCapacitaSchema.d_data_a), dateFormatToExport))
      .select(AnagraficaSchema.t_codice_pdr, ClgPdrCapacitaSchema.d_data_a)
      .distinct()

    val dfPdrDa = df
      .withColumn("first", lag(numericDay, 1) over window)
      .filter(col("first").isNull)
      .withColumn(ClgPdrCapacitaSchema.d_data_da, col(numericDay))
      .withColumn(ClgPdrCapacitaSchema.d_data_da, to_date(unix_timestamp(col(ClgPdrCapacitaSchema.d_data_da).cast(StringType), "yyyyMMdd").cast(TimestampType)))
      .withColumn(ClgPdrCapacitaSchema.d_data_da, date_format(col(ClgPdrCapacitaSchema.d_data_da), dateFormatToExport))
      .select(AnagraficaSchema.t_codice_pdr, ClgPdrCapacitaSchema.d_data_da)
      .distinct()

    val c: String = AnagraficaSchema.t_codice_pdr
    dfPdrA
      .join(dfPdrDa, Seq(c), "inner")
      .select(AnagraficaSchema.t_codice_pdr, ClgPdrCapacitaSchema.d_data_da, ClgPdrCapacitaSchema.d_data_a)

  }

  override protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame = {

        val dataFrame_v2=dataFrame.getNumericDay()
        val w=Window.partitionBy(AnagraficaSchema.t_codice_pdr)
        val caMaxForPdr = dataFrame_v2
          .withColumn(caMax, max(CalcoloConsumiSbgSchema.consumo) over w)
          .dropDuplicates(Seq(AnagraficaSchema.t_codice_pdr.toString))

        val c: String = AnagraficaSchema.t_codice_pdr
        val toReturn = caMaxForPdr.join(calculateDaAPeriod(dataFrame_v2), Seq(c), "left")
        if (args.verbose) {
          LoggerUtility.printInfo("I finished with StandardFlow ", getClass.getName)
        }
        Transformation.setPcmFields(toReturn, typeCalc2Value)
  }

  override protected def getSchemaPreCalculation: List[String] =
    (CalcoloConsumiSbgSchema.getValues - CalcoloConsumiSbgSchema.cod_pdr)::: List(annoMeseGiornoDate) :::
      AnagraficaSchema.getValues

  override protected def getFieldCalculated1: String = ClgPdrCapacitaSchema.n_pcm

  override protected def getFieldCalculated2: String = ClgPdrCapacitaSchema.t_tipo_calcolo

  override protected def getFieldCalculated3: String = ClgPdrCapacitaSchema.d_data_da

  override protected def getFieldCalculated4: String = ClgPdrCapacitaSchema.d_data_a
}
