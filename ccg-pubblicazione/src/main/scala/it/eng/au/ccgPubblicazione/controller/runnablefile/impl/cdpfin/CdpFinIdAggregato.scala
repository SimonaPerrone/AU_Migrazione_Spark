package it.eng.au.ccgPubblicazione.controller.runnablefile.impl.cdpfin

import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.cdp.CdpPdrElencoFlussi
import it.eng.au.ccgPubblicazione.schema.cdp.CdpConsumptionRequestRunnableSchema
import it.eng.au.ccgPubblicazione.schema.cdp.output.{CdpConsumiOutputSchema, CdpFlussiOutputSchema}
import it.eng.au.ccgPubblicazione.utility.Constants.{CDP, CDP_FIN, FIN}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object CdpFinIdAggregato extends CdpPdrElencoFlussi {
  override val publicationType: String = FIN
  override val baseNumber: String = "2"

  override val keyFieldsConsumi: ListMap[String, String] = ListMap(
    idrichiesta -> CdpConsumptionRequestRunnableSchema.idRichiesta.toString,
    piva -> CdpConsumptionRequestRunnableSchema.piva_distr.toString,
    periodoCompetenza -> CdpConsumptionRequestRunnableSchema.anno_competenza.toString
  )
  override val keyFieldsFlussi: ListMap[String, String] = ListMap(
    idrichiesta -> CdpConsumptionRequestRunnableSchema.idRichiesta.toString,
    piva -> CdpConsumptionRequestRunnableSchema.piva_distr.toString,
    periodoCompetenza -> CdpFlussiOutputSchema.AT.toString
  )

  override def fileSpecificFilterExpression: Column = col(CdpConsumptionRequestRunnableSchema.piva_distr).isNotNull and col(CdpConsumptionRequestRunnableSchema.idRichiesta).isNotNull and col(CdpConsumptionRequestRunnableSchema.anno_competenza).isNotNull
}


