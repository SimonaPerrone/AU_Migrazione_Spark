package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IncoerentiDettaglioSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerentiSbg, RdbElencoFlussiIncoerentiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr.{PdrDettaglioIncoerentiSbg, RdbPdrIncoerentiDettaglioSbg}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object RdbIncoerentiDettaglioSbg extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "2"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerentiSbg = RdbPdrIncoerentiDettaglioSbg
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerentiSbg = RdbElencoFlussiIncoerentiDettaglioSbg
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_rdb)
  override val mainPiva: String = keyFields.head

  override def fileSpecificFilterExpression: Column = {
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
      col(DailyConsumptionAggSchema.pivaRdb).isNotNull and
      (col(DailyConsumptionAggSchema.pivaIt) =!= lit("10238291008"))
  }
}
