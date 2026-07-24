package it.au.misure.calcolo_capacita.component.implementation.joinintersection

import it.au.misure.calcolo_capacita.component.contract.Join
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.annoMeseGiornoDate
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ListUtility.ListUtility
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col}


object JoinMeasure$Measure extends Join {

  override protected def calculation(measureDfWithF1: DataFrame, measureDf: DataFrame)(implicit args: Args): DataFrame = {

    val f1 = measureDfWithF1.select(AnagraficaSchema.t_codice_pdr).distinct()
      .withColumnRenamed(AnagraficaSchema.t_codice_pdr, "flag")
    val condition = f1("flag") === measureDf(AnagraficaSchema.t_codice_pdr)
    measureDf.join(broadcast(f1), condition, "left")
      .filter(col("flag").isNull)

  }

  override protected def getFieldsAfterJoin: List[String] =
    (CalcoloConsumiSbgSchema.getValues - CalcoloConsumiSbgSchema.cod_pdr)::: List(annoMeseGiornoDate) :::
      AnagraficaSchema.getValues
}
