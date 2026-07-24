package it.au.misure.calcolo_capacita.component.implementation.checkfieldcalculation

import it.au.misure.calcolo_capacita.component.contract.FieldCalculation
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant._
import it.au.misure.calcolo_capacita.component.utility.CalculatedField._
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ListUtility._
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FieldCalculationTrattamentoMisure extends FieldCalculation {

  override def getSchemaPreCalculation: List[String] =
    (CalcoloConsumiSbgSchema.getValues - CalcoloConsumiSbgSchema.cod_pdr)::: List(annoMeseGiornoDate) :::
      AnagraficaSchema.getValues

  override protected def getFieldCalculated: String = PATH_CHECK_TRATTAMENTO

  override protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame = {

    val w = Window.partitionBy(AnagraficaSchema.t_codice_pdr)

    val countMisureTot = count("*") over w

    val countMisureEqual = when(col(CalcoloConsumiSbgSchema.trattamento) isin(Y, M), lit(1)).otherwise(0)

    val calculateZ = sum(Ztemp) over w

    val generatePath = when(col(Z) === lit(0), lit(PATH$OK$PRESENTI$Z$EQ$0$OK))
      .otherwise(when(col(Z) === col(TOT), lit(PATH$OK$PRESENTI$Z$EQ$TOT$OK))
        .otherwise(when(col(TOT) > col(Z) and col(Z) > lit(0), PATH$OK$PRESENTI$TOT$GR$Z$GR$0)))

    val toReturn = dataFrame
      //TOT= numero totale di misurazioni per pdr
      .withColumn(TOT, countMisureTot)
      .withColumn(Ztemp, countMisureEqual)
      .withColumn(Z, calculateZ)
      .withColumn(getFieldCalculated, generatePath)

    toReturn
  }
}
