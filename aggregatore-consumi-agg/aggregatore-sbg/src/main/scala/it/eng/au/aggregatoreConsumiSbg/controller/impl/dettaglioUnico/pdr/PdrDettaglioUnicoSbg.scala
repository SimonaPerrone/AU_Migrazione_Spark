package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr.PdrDettaglioUnico
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

trait PdrDettaglioUnicoSbg extends PdrDettaglioUnico {
  /** [AU-603] Per ora non ci hanno richiesto di implementare il filtro anche per SBG */
  override def specificFilterForIncoerentiGdm: Column = lit(true)
}
