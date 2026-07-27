package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusi, RdbElencoFlussiEsclusiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.pdr.{PdrDettaglioEsclusi, RdbPdrEsclusiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.EsclusiTrait
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, EsclusiOutputSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object RdbEsclusi extends EsclusiTrait {
  override val baseNumber: String = "2"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusi = RdbPdrEsclusiDettaglio
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusi = RdbElencoFlussiEsclusiDettaglio
  override val keyPiva1: String = EsclusiOutputSchema.piva_rdb
  override val keyPiva2: String = EsclusiOutputSchema.piva_rdb
  override val mainPiva: String = keyPiva1
}
