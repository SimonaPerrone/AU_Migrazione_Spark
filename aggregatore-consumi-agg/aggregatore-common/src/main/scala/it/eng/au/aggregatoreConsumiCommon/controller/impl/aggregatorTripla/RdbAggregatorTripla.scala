package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla

import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.dettaglio.{PdrDettaglioAggregatoTripla, RdbDettaglioAggregatoTripla}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.elencoRemi.{ElencoRemiDettaglioAggregatoTripla, RdbElencoRemiDettaglioAggregatoTripla}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.AggregatorTriplaTrait
import it.eng.au.aggregatoreConsumiCommon.schema.AggregatoTriplaSchema

object RdbAggregatorTripla extends AggregatorTriplaTrait {
  override val baseNumber: String = "2"
  override val pdrDettaglioAggregatoTripla: PdrDettaglioAggregatoTripla = RdbDettaglioAggregatoTripla
  override val elencoFlussiDettaglioAggregatoTripla: ElencoRemiDettaglioAggregatoTripla = RdbElencoRemiDettaglioAggregatoTripla
  override val keyFields: List[String] = List(AggregatoTriplaSchema.piva_rdb)
  override val mainPiva: String = keyFields.head
}
