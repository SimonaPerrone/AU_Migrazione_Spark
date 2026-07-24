package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculation1
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{PATH, PATH_CHECK_TRATTAMENTO, annoMeseGiornoDate}
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ListUtility.ListUtility
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object FieldCalculation1PathOnMeasure extends FieldCalculation1 {

  override protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    dataFrame.withColumn(PATH, col(PATH_CHECK_TRATTAMENTO))

  }

  override protected def getFieldCalculated1: String = PATH

  override protected def getSchemaPreCalculation: List[String] =
    (CalcoloConsumiSbgSchema.getValues - CalcoloConsumiSbgSchema.cod_pdr)::: List(annoMeseGiornoDate,PATH_CHECK_TRATTAMENTO) :::
      AnagraficaSchema.getValues

}
