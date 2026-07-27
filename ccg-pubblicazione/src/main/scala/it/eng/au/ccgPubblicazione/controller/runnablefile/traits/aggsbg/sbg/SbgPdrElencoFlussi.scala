package it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg.sbg

import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg.AggSbgPdrElencoFlussi
import it.eng.au.ccgPubblicazione.schema.aggsbg.AggConsumptionRequestRunnableSchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait SbgPdrElencoFlussi extends AggSbgPdrElencoFlussi {
  override def specificFilterForIncoerentiGdm: Column = col(AggConsumptionRequestRunnableSchema.treatment).isin("G","M")
}