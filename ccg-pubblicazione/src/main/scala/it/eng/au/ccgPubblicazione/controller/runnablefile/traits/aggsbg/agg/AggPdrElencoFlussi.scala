package it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg.agg

import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg.AggSbgPdrElencoFlussi
import it.eng.au.ccgPubblicazione.schema.aggsbg.AggConsumptionRequestRunnableSchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

trait AggPdrElencoFlussi extends AggSbgPdrElencoFlussi {
  override def specificFilterForIncoerentiGdm: Column = lit(true)//col(AggConsumptionRequestRunnableSchema.treatment).isin("G","M","Y")
}
