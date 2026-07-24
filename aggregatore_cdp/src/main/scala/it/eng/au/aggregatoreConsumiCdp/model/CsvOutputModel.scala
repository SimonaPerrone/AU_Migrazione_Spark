package it.eng.au.aggregatoreConsumiCdp.model

import it.eng.au.aggregatoreConsumiCdp.utility.Constants.CSV_SEPARATOR

case class CsvOutputModel(
                           cod_pdr: String,
                           cod_remi: String,
                           cat_uso: String,
                           classe_prelievo: String,
                           zona_climatica: String,
                           id_reg_clim: String,
                           cod_prof_prel_std: String,
                           prelievo_annuo_prev: String,
                           trattamento: String,
                           data_decorrenza: String
                         ) {

  def toStringRow: String = List(
    cod_pdr,
    cod_remi,
    cat_uso,
    classe_prelievo,
    zona_climatica,
    id_reg_clim,
    cod_prof_prel_std,
    prelievo_annuo_prev,
    trattamento,
    data_decorrenza).mkString(CSV_SEPARATOR)
}

object CsvOutputModel {
  def header: String = List(
    "COD_PDR",
    "COD_REMI",
    "CAT_USO",
    "CLASSE_PRELIEVO",
    "ZONA_CLIMATICA",
    "ID_REG_CLIM",
    "COD_PROF_PREL_STD",
    "PRELIEVO_ANNUO_PREV",
    "TRATTAMENTO",
    "DATA_DECORRENZA",
    "SESSIONE",
    "CAUSALE"
  ).mkString(CSV_SEPARATOR)
}



