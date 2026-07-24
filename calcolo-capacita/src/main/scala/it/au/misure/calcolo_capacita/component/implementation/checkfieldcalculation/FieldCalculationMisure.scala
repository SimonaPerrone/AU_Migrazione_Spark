package it.au.misure.calcolo_capacita.component.implementation.checkfieldcalculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculation
import it.au.misure.calcolo_capacita.component.schema.AnagraficaSchema
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{PATH$OK$NONPRESENTI, PATH$OK$PRESENTI}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{PATH_CHECK_FORNITURA, PATH_CHECK_MISURE, flagAnagrafica}
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

object FieldCalculationMisure extends FieldCalculation {

  override protected def getFieldCalculated: String = PATH_CHECK_MISURE

  override protected def calculation(measure$Anagrafica: DataFrame)(implicit args: Args): DataFrame = {

    measure$Anagrafica
      .withColumn(getFieldCalculated,
        when(col(flagAnagrafica) isNull, lit(PATH$OK$NONPRESENTI)).otherwise(lit(PATH$OK$PRESENTI)))
  }

  override protected def getSchemaPreCalculation: List[String] = AnagraficaSchema.getValues ::: List(PATH_CHECK_FORNITURA,flagAnagrafica)
}
