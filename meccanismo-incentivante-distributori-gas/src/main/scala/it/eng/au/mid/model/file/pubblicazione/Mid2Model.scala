package it.eng.au.mid.model.file.pubblicazione

import it.eng.au.mid.schema.file.pubblicazione.Mid2Schema

/**
 * Contenuto file CSV MID2
 */
case class Mid2Model(
                      PIVA_DISTR: String,
                      RAGIONE_SOCIALE_DISTR: String,
                      STATO_DISTR: String,
                      PIVA_UDD: String,
                      RAGIONE_SOCIALE_UDD: String,
                      PDR: String,
                      ANNOMESE: String,
                      N: Int,
                      COD_REMI: String,
                      GDM: String,
                      ALPHA: Int,
                      PIVA_DISTR_ATT: String,
                      RAGIONE_SOCIALE_DISTR_ATT: String
                    ) {

  override def toString: String = toString(sep = ",")

  def toString(sep: String): String = {
    Seq(PIVA_DISTR, RAGIONE_SOCIALE_DISTR, PIVA_UDD, RAGIONE_SOCIALE_UDD, PDR, ANNOMESE, N, COD_REMI, GDM, ALPHA).mkString(sep)
  }
}

object Mid2Model {
  def header(sep: String = ","): String = {
    Mid2Schema.getValues.mkString(sep)
  }
}
