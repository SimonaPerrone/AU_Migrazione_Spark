package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.schema.ClgPdrCapacitaSchema
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{KO, OK, WARN_CONT_FORN}
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object FieldCalculationNResultTablePathKo extends FieldCalculationNResultTable {

  override protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    val dataFrame_v2 = super.calculation(dataFrame)
    dataFrame_v2
      .withColumn(ClgPdrCapacitaSchema.t_esito_calcolo, lit(OK))
      .withColumn(ClgPdrCapacitaSchema.t_esito_code_desc, lit(WARN_CONT_FORN))

  }

}
