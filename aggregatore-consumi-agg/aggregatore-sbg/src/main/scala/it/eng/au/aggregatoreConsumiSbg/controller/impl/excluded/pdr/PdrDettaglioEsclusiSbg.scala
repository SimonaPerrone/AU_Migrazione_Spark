package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.pdr.PdrDettaglioEsclusi

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait PdrDettaglioEsclusiSbg extends PdrDettaglioEsclusi {

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_SBG_${operationName}_${annomese}_${timestamp}_${counterCsv}.csv"
  }

}
