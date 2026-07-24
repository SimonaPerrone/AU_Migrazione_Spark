package it.au.misure.calcolo_capacita.component.implementation.joinintersection

import it.au.misure.calcolo_capacita.component.contract.Join
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{PATH_CHECK_FORNITURA, flagAnagrafica}
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame

/**
 * 1. JOIN TRA ANAGRAFICA E MISURE PER RECUPERARE I PDR PRESENTI SOLO IN ANAGRAFICA (DATAFRAME CON SOLO COD_PDR)
 * 2. JOIN ANAGRAFICA E .1 PER AFFIANCARE IL FLAG CHE INDICA LA PRESENZA NELLA SOLO ANAGRAFICA
 * RISULTATO: ANAGRAFICA COMPLETA CON flagAnagrafica che indica se il pdr è presente solo in anagrafica
 */
object JoinAnagrafica$Measure extends Join {

  override protected def calculation(anagraficaDf: DataFrame, measureDf: DataFrame)(implicit args: Args): DataFrame = {

    val measurePdr=measureDf
      .select(CalcoloConsumiSbgSchema.cod_pdr).distinct()
      .withColumnRenamed(CalcoloConsumiSbgSchema.cod_pdr,"flag")

    val condition = measurePdr("flag") === anagraficaDf(AnagraficaSchema.t_codice_pdr)
    anagraficaDf.join(measurePdr,condition,"left")
      .withColumnRenamed("flag",flagAnagrafica)

  }

  override protected def getFieldsAfterJoin: List[String] = AnagraficaSchema.getValues ::: List(PATH_CHECK_FORNITURA, flagAnagrafica)
}
