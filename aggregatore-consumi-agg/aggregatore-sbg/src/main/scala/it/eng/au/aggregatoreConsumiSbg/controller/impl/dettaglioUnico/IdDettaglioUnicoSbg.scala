package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.controller.traits.DettaglioUnicoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DettaglioUnicoSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.elencoFlussi.{ElencoFlussiDettaglioUnicoSbg, IdElencoFlussiDettaglioUnicoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.pdr.{IdPdrDettaglioUnicoSbg, PdrDettaglioUnicoSbg}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object IdDettaglioUnicoSbg extends DettaglioUnicoTrait {
  override val baseNumber: String = "4"
  override val pdrDettaglioUnico: PdrDettaglioUnicoSbg = IdPdrDettaglioUnicoSbg
  override val elencoFlussiDettaglioUnico: ElencoFlussiDettaglioUnicoSbg = IdElencoFlussiDettaglioUnicoSbg
  override val keyPiva1: String = DettaglioUnicoSchema.piva_distr
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd
  override val mainPiva: String = keyPiva1

  override def getZipOutputName(pivaFolder: String, annomese: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_SBG_202204_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${sessionName}_${year}_${timestamp}_1.zip"
    zipName
  }
}
