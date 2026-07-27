package it.eng.au.aggregatoreConsumiSbg.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.traits.DettaglioGTrait
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait DettaglioGTraitSbg extends DettaglioGTrait {
  /** [AU-603] Per ora non ci hanno richiesto di implementare il filtro anche per SBG */
  override def specificFilterForIncoerentiGdm: Column = lit(true)

  override def getZipOutputName(pivaFolder: String, publicationType: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_SBG_G_202204_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${publicationType}_G_${year}_${timestamp}_1.zip"
    zipName
  }
}
