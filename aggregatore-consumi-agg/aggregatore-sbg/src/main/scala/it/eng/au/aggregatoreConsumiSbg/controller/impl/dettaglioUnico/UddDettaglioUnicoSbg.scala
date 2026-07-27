package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi.{ElencoFlussiDettaglioUnico, UddElencoFlussiDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr.PdrDettaglioUnico
import it.eng.au.aggregatoreConsumiCommon.controller.traits.DettaglioUnicoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DettaglioUnicoSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.elencoFlussi.{ElencoFlussiDettaglioUnicoSbg, UddElencoFlussiDettaglioUnicoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.pdr.{PdrDettaglioUnicoSbg, UddPdrDettaglioUnicoSbg}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object UddDettaglioUnicoSbg extends DettaglioUnicoTrait {
  override val baseNumber: String = "1"
  override val pdrDettaglioUnico: PdrDettaglioUnicoSbg = UddPdrDettaglioUnicoSbg
  override val elencoFlussiDettaglioUnico: ElencoFlussiDettaglioUnicoSbg = UddElencoFlussiDettaglioUnicoSbg
  override val keyPiva1: String = DettaglioUnicoSchema.piva_udd
  override val keyPiva2: String = DettaglioUnicoSchema.piva_distr
  override val mainPiva: String = keyPiva1

  override def getZipOutputName(pivaFolder: String, annomese: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_SBG_202204_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${sessionName}_${year}_${timestamp}_1.zip"
    zipName
  }

}
