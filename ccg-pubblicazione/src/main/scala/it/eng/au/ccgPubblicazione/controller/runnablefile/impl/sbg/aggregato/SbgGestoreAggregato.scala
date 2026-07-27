package it.eng.au.ccgPubblicazione.controller.runnablefile.impl.sbg.aggregato

import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg.sbg.SbgPdrElencoFlussi
import it.eng.au.ccgPubblicazione.schema.aggsbg.AggConsumptionRequestRunnableSchema
import it.eng.au.ccgPubblicazione.schema.aggsbg.output.AggFlussiOutputSchema
import it.eng.au.ccgPubblicazione.utility.Constants.SBG
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object SbgGestoreAggregato extends SbgPdrElencoFlussi {
  override val operationName: String = SBG
  override val baseNumber: String = "4"

  override val keyFieldsConsumi: ListMap[String, String] = ListMap(
    idrichiesta -> AggConsumptionRequestRunnableSchema.idRichiesta.toString,
    piva -> AggConsumptionRequestRunnableSchema.pivaGestore.toString,
    periodoCompetenza -> AggConsumptionRequestRunnableSchema.annoMese.toString
  )
  override val keyFieldsFlussi: ListMap[String, String] = ListMap(
    idrichiesta -> AggConsumptionRequestRunnableSchema.idRichiesta.toString,
    piva -> AggConsumptionRequestRunnableSchema.pivaGestore.toString,
    periodoCompetenza -> AggFlussiOutputSchema.annomese.toString
  )

  override val keyFiledsPreRenamed: ListMap[String, String] = ListMap(
    idrichiesta -> AggConsumptionRequestRunnableSchema.idRichiesta.toString,
    piva -> AggConsumptionRequestRunnableSchema.pivaGestore.toString,
    periodoCompetenza -> AggConsumptionRequestRunnableSchema.annoMese.toString
  )

  override def fileSpecificFilterExpression: Column = col(AggConsumptionRequestRunnableSchema.pivaGestore).isNotNull and col(AggConsumptionRequestRunnableSchema.idRichiesta).isNotNull and col(AggConsumptionRequestRunnableSchema.annoMese).isNotNull

  override val isAnno: Boolean = false
  override val mesiInAnno: String = ""

}
