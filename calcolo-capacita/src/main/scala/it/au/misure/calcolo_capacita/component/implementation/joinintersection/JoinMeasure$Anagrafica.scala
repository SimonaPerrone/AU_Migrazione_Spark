package it.au.misure.calcolo_capacita.component.implementation.joinintersection

import it.au.misure.calcolo_capacita.component.contract.Join
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.annoMeseGiornoDate
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ListUtility._
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast

/** *
 * INNER JOIN TRA ANAGRAFICA E MEASURE
 */
object JoinMeasure$Anagrafica extends Join {

  override protected def calculation(anagraficaDf: DataFrame, measureDf: DataFrame)(implicit args: Args): DataFrame = {

    val condition = measureDf(CalcoloConsumiSbgSchema.cod_pdr) === anagraficaDf(AnagraficaSchema.t_codice_pdr)
    measureDf.join(broadcast(anagraficaDf), condition, "inner")

  }

  override protected def getFieldsAfterJoin: List[String] =
    (CalcoloConsumiSbgSchema.getValues - CalcoloConsumiSbgSchema.cod_pdr) ::: List(annoMeseGiornoDate) :::
      AnagraficaSchema.getValues
}
