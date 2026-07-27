package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.controller.traits.DettaglioUnicoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DettaglioUnicoSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.elencoFlussi.{ElencoFlussiDettaglioUnicoSbg, UdbElencoFlussiDettaglioUnicoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.pdr.{PdrDettaglioUnicoSbg, UdbPdrDettaglioUnicoSbg}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object UdbDettaglioUnicoSbg extends DettaglioUnicoTrait {
  override val baseNumber: String = "5"
  override val pdrDettaglioUnico: PdrDettaglioUnicoSbg = UdbPdrDettaglioUnicoSbg
  override val elencoFlussiDettaglioUnico: ElencoFlussiDettaglioUnicoSbg = UdbElencoFlussiDettaglioUnicoSbg
  override val keyPiva1: String = DettaglioUnicoSchema.Piva_Udb
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd
  override val mainPiva: String = keyPiva1

  override def getZipOutputName(pivaFolder: String, annomese: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_SBG_202204_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${sessionName}_${year}_${timestamp}_1.zip"
    zipName
  }
}
