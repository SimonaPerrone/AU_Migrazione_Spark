package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculationN
import it.au.misure.calcolo_capacita.component.implementation.Calculation
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.processoLabelConstant
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.component.utility.property.ApplicationProperty.dateFormatToExport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit}
import org.apache.spark.sql.types.StringType

class FieldCalculationNResultTable extends FieldCalculationN {

  override protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    val dataCalc = args.dataCalc

    val dDataInizio=Calculation.calculateDataInizio()
    val monthOfYearDDataCalc = dataCalc.getMonthOfYear
    val annoTermico=Calculation.calculateAnnoTermico(dDataInizio)
    val nullCol = lit(null).cast(StringType)


    val toReturn = dataFrame
      .withColumn(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp, nullCol)
      .withColumn(ClgPdrCapacitaSchema.n_id_pdr, col(AnagraficaSchema.n_id_pdr))
      .withColumnRenamed(AnagraficaSchema.t_codice_pdr, ClgPdrCapacitaSchema.t_codice_pdr)
      .withColumn(ClgPdrCapacitaSchema.d_data_rif, date_format(lit(dataCalc.toString("yyyy-MM-dd")), dateFormatToExport))
      .withColumn(ClgPdrCapacitaSchema.d_data_inizio,  date_format(lit(dDataInizio.toString("yyyy-MM-dd")), dateFormatToExport))
      .withColumn(ClgPdrCapacitaSchema.t_origine, lit("spark-job-calcolo-capacita"))
      .withColumn(ClgPdrCapacitaSchema.d_data_inserimento, date_format(current_timestamp().as(ClgPdrCapacitaSchema.d_data_inserimento), dateFormatToExport))
      .withColumn(ClgPdrCapacitaSchema.t_esito_agg_rcu, nullCol)
      .withColumn(ClgPdrCapacitaSchema.t_errore_agg_rcu, nullCol)
      .withColumn(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc, nullCol)
      .withColumn(ClgPdrCapacitaSchema.t_stato, nullCol)
      .withColumn(ClgPdrCapacitaSchema.d_data_aggiornamento, nullCol)
      .withColumn(ClgPdrCapacitaSchema.n_execution_id, lit(args.executionId))
      .withColumn(ClgPdrCapacitaSchema.n_ctc, col(ClgPdrCapacitaSchema.n_pcm) * col(AnagraficaSchema.t_z))
      .withColumn(ClgPdrCapacitaSchema.t_processo_origine, lit(processoLabelConstant))
      .withColumn(ClgPdrCapacitaSchema.n_id_pratica, nullCol)
      .withColumn(ClgPdrCapacitaSchema.n_anno, lit(annoTermico))
      .withColumn(ClgPdrCapacitaSchema.n_mese, lit(monthOfYearDDataCalc))

    val tmp = "tmp"
    toReturn
      .withColumn(tmp, col(ClgPdrCapacitaSchema.d_data_da))
      .withColumn(ClgPdrCapacitaSchema.d_data_da, col(ClgPdrCapacitaSchema.d_data_a))
      .withColumn(ClgPdrCapacitaSchema.d_data_a, col(tmp))
      .drop(tmp)
  }

  override protected def getFieldCalculated: List[String] = ClgPdrCapacitaSchema.getValues

  override protected def getSchemaPreCalculation: List[String] =
    AnagraficaSchema.getValues :::
      List[String](ClgPdrCapacitaSchema.n_pcm, ClgPdrCapacitaSchema.t_tipo_calcolo, ClgPdrCapacitaSchema.d_data_da, ClgPdrCapacitaSchema.d_data_a)

}
