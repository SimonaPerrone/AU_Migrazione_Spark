package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculation1
import it.au.misure.calcolo_capacita.component.schema.AnagraficaSchema
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.PATH$KO
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{PATH, PATH_CHECK_FORNITURA, PATH_CHECK_MISURE, flagAnagrafica}
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}


object FieldCalculation1PathOnAnagrafica extends FieldCalculation1 {

  override protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame = {

    val cond = when(col(PATH_CHECK_FORNITURA) === lit(PATH$KO), lit(PATH$KO))
      .otherwise(col(PATH_CHECK_MISURE))
    dataFrame.withColumn(PATH, cond)
  }

  override protected def getFieldCalculated1: String = PATH

  override protected def getSchemaPreCalculation: List[String] =
    AnagraficaSchema.getValues ::: List(PATH_CHECK_FORNITURA,PATH_CHECK_MISURE,flagAnagrafica)
}