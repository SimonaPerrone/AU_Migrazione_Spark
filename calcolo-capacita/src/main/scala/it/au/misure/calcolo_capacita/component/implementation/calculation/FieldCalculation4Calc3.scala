package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculation4
import it.au.misure.calcolo_capacita.component.implementation.Transformation
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.typeCalc3Value
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.caMax
import it.au.misure.calcolo_capacita.component.utility.`object`.Range
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.component.utility.property.ApplicationProperty.dateFormatToExport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, lit}

object FieldCalculation4Calc3 extends FieldCalculation4 {


  override protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    val range = Range(args.dataCalc, args.y)
    val rigth = range.rigth
    val left = range.left
    val toReturn = dataFrame
      //Questa drop è necessaria perchè potrebbero anche record di misure per un certo pdr, ovviamente con la componente
      //di anagrafica uguale, poichè qui il calcol lo faccio sulle colonne di anagrafica, la distinct è possibile
      .dropDuplicates(Seq(AnagraficaSchema.t_codice_pdr.toString))
      .withColumn(caMax, col(AnagraficaSchema.n_prelievo_annuo) * col(AnagraficaSchema.t_pmax))
      .withColumn(ClgPdrCapacitaSchema.d_data_da, date_format(lit(rigth.toString("yyyy-MM-dd")), dateFormatToExport))
      .withColumn(ClgPdrCapacitaSchema.d_data_a, date_format(lit(left.toString("yyyy-MM-dd")), dateFormatToExport))
    Transformation.setPcmFields(toReturn, typeCalc3Value)
  }

  override protected def getFieldCalculated1: String = ClgPdrCapacitaSchema.n_pcm

  override protected def getFieldCalculated2: String = ClgPdrCapacitaSchema.t_tipo_calcolo

  override protected def getFieldCalculated3: String = ClgPdrCapacitaSchema.d_data_da

  override protected def getFieldCalculated4: String = ClgPdrCapacitaSchema.d_data_a

  override protected def getSchemaPreCalculation: List[String] = AnagraficaSchema.getValues
}
