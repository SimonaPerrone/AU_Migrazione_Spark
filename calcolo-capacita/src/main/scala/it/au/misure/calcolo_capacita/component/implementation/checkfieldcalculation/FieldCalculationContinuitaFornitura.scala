package it.au.misure.calcolo_capacita.component.implementation.checkfieldcalculation


import it.au.misure.calcolo_capacita.component.contract.FieldCalculation
import it.au.misure.calcolo_capacita.component.schema.AnagraficaSchema
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{PATH$KO, PATH$OK}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{PATH_CHECK_FORNITURA, flagRcuGasMassivo}
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

object FieldCalculationContinuitaFornitura extends FieldCalculation {

  override protected def getFieldCalculated: String = PATH_CHECK_FORNITURA

  override protected def calculation(anagrafica$Rcu: DataFrame)(implicit args: Args): DataFrame = {

    anagrafica$Rcu
      .withColumn(getFieldCalculated,
        when(col(flagRcuGasMassivo) isNotNull, lit(PATH$KO)).otherwise(lit(PATH$OK)))
  }

  override protected def getSchemaPreCalculation: List[String] = AnagraficaSchema.getValues ::: List(flagRcuGasMassivo)
}
